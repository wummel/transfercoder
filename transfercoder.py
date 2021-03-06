#!/usr/bin/env python3

import os
import os.path
import subprocess
import shutil
from warnings import warn
import sys
import re
import collections
import argparse
import quodlibet
quodlibet.init_cli()
from quodlibet.formats import MusicFile
import multiprocessing
import logging
from distutils.spawn import find_executable
# Use the built-in version of scandir/walk if possible, otherwise
# use the scandir module version
try:
    from os import scandir, walk
except ImportError:
    from scandir import scandir, walk

rsync_exe = find_executable("rsync")
pacpl_exe = find_executable("pacpl")

def default_job_count():
    try:
        return multiprocessing.cpu_count()
    except Exception:
        return 1

def call_checked(cmd, *args, **kwargs):
    """Run subprocess.check_output to get output in case of errors.
    Stdin is read from the null device.
    Raises subprocess.CalledProcessError on errors."""
    nullsrc = open(os.devnull, "r")
    logging.debug("Calling command: %s", repr(cmd))
    return subprocess.check_output(cmd, *args, stdin=nullsrc, stderr=subprocess.STDOUT, **kwargs)

def filter_hidden(paths):
    return [x for x in paths if x and x[0] != "."]

def splitext_afterdot(path):
    """Same as os.path.splitext, but the dot goes to the base."""
    base, ext = os.path.splitext(path)
    if len(ext) > 0 and ext[0] == ".":
        base += "."
        ext = ext[1:]
    return (base, ext)

class AudioFile(collections.MutableMapping):
    """A simple class just for tag editing.

    No internal mutagen tags are exposed, or filenames or anything. So
    calling clear() won't destroy the filename field or things like
    that. Use it like a dict, then .write() it to commit the changes.

    Or grab the actual underlying quodlibet format object from the
    .data field and get your hands dirty."""

    # A list of regexps matching non-transferrable tags, like file format
    # info and replaygain info. This will not be transferred from source,
    # nor deleted from destination. They will effectively be hidden, nether
    # settable nor gettable.
    blacklist_regexes = [re.compile(s) for s in (
        'encoded',
        'replaygain',
        # Exclude mutagen's internal tags
        '^~',
        )
    ]

    def __init__(self, filename):
        self.data = MusicFile(filename)

    def __getitem__(self, item):
        if self.blacklisted(item):
            warn("%s is a blacklisted key." % item)
        else:
            return self.data[item]

    def __setitem__(self, item, value):
        if self.blacklisted(item):
            warn("%s is a blacklisted key." % item)
        else:
           self.data[item] = value

    def __delitem__(self, item):
        if self.blacklisted(item):
            warn("%s is a blacklisted key." % item)
        else:
            del self.data[item]

    def __iter__(self):
        return (key for key in self.data if not self.blacklisted(key))

    def __len__(self):
        return len([key for key in self.data if not self.blacklisted(key)])

    def blacklisted(self, item):
        """Return True if tag is blacklisted.

        Blacklist automatically includes internal mutagen tags (those
        beginning with a tilde)."""
        return any(re.search(regex, item) for regex in self.blacklist_regexes)

    def write(self):
        return self.data.write()


def copy_tags (src, dest):
    """Replace tags of dest file with those of src.

Excludes format-specific tags and replaygain info, which does not
carry across formats."""
    try:
        m_src = AudioFile(src)
        m_dest = AudioFile(dest)
        m_dest.clear()
        m_dest.update(m_src)
        m_dest.write()
    except Exception:
        # It's ok if copying the tags fails, but print a warning.
        logging.warn("Could not copy tags from %s -> %s", src, dest, exc_info=sys.exc_info())

def copy_mode(src, dest):
    """Copy file mode. Is allowed to fail since some network filesystems
    (eg. CIFS) don't allow those mode settings."""
    try:
        shutil.copymode(src, dest)
    except OSError:
        # It's ok if setting the mode fails, but print a warning.
        logging.warn("Could not copy file mode from %s -> %s", src, dest, exc_info=sys.exc_info())

class Transfercode(object):
    def __init__(self, src, dest, eopts=None):
        self.src = src
        self.dest = dest
        self.src_dir = os.path.split(self.src)[0]
        self.dest_dir = os.path.split(self.dest)[0]
        self.src_ext = splitext_afterdot(self.src)[1]
        self.dest_ext = splitext_afterdot(self.dest)[1]
        self.eopts = eopts

    def __repr__(self):
        return "%s(%s, %s, %s)" % (type(self).__name__, repr(self.src), repr(self.dest), repr(self.eopts))

    def __str__(self):
        return repr(self)

    def needs_update(self):
        """Returns true if dest file needs update.

        This is true when the dest file does not exist, or exists but
        is older than the source file."""
        if not os.path.exists(self.dest):
            return True
        src_mtime = os.path.getmtime(self.src)
        dest_mtime = os.path.getmtime(self.dest)
        return src_mtime > dest_mtime

    def needs_transcode(self):
        return self.src_ext != self.dest_ext

    def transcode(self, dry_run=False):
        logging.info("Transcoding: %s -> %s", self.src, self.dest)
        if '"' in self.src:
            # pacpl's quoting does not support double quotes in the filename
            # since it just surrounds the filename with double quotes itself.
            raise Exception("Double quote in %r is not supported by pacpl. "
                            "Rename the file and remove the double quote."
                            % self.src)
        if dry_run:
            return
        # pacpl expects a relative path with no extension, apparently
        rel_dest = os.path.relpath(self.dest, self.src_dir)
        rel_dest_base = os.path.splitext(rel_dest)[0]
        command = [pacpl_exe] + (["--eopts", self.eopts] if self.eopts else []) + \
          ["--bitrate=160", "--overwrite", "--keep", "--to", self.dest_ext,
           "--outfile", rel_dest_base, self.src]
        call_checked(command)
        if not os.path.isfile(self.dest):
            raise Exception("Perl Audio Converter did not produce an output file")
        copy_tags(self.src, self.dest)
        copy_mode(self.src, self.dest)

    def copy(self, dry_run=False):
        """Copy src to dest, trying hard linking and rsync first,
        then normal file copy."""
        logging.info("Copying: %s -> %s", self.src, self.dest)
        if dry_run:
            return
        success = False
        try:
            os.link(self.src, self.dest)
            success = True
        except OSError:
            pass
        if not success and rsync_exe:
            try:
                call_checked([rsync_exe, "-q", "-p", self.src, self.dest])
                success = True
            except Exception:
                success = False
        if not success:
            # Try regular copy instead if rsync is not available or failed
            shutil.copyfile(self.src, self.dest)
            copy_mode(self.src, self.dest)

    def check(self):
        """Checks that source file and dest dir exist.

        Throws IOError if not. This is called just before initiating
        transfer."""
        if not os.path.isfile(self.src):
            raise IOError("Missing input file: %s" % self.src)
        elif not os.path.isdir(self.dest_dir):
            raise IOError("Missing output directory: %s" % self.dest_dir)

    def transfer(self, force=False, dry_run=False):
        """Copies or transcodes src to dest.
        Destination directory must already exist.
        Optional arg force performs the transfer even if dest is newer.
        Optional arg dry_run skips the actual action."""
        error = 0
        if force or self.needs_update():
            try:
                if not dry_run:
                    self.check()
                if self.needs_transcode():
                    self.transcode(dry_run=dry_run)
                else:
                    self.copy(dry_run=dry_run)
            except Exception:
                logging.exception("Error transfering %s -> %s", self.src, self.dest)
                error = 1
        else:
            logging.debug("Skipping: %s -> %s", self.src, self.dest)
        return error

def is_subpath(path, parent):
    """Returns true if path is a subpath of parent.

    For example, '/usr/bin/python' is a subpath of '/usr', while
    '/bin/ls' is not. Both paths must be absolute, but no checking is
    done. Behavior on relative paths is undefined.

    Any path is a subpath of itself."""
    # Any relative path that doesn't start with ".." is a subpath.
    return not os.path.relpath(path, parent)[0:2].startswith(os.path.pardir)

def walk_files(dir, hidden=False):
    """Iterator over paths to non-directory files in dir.

    The returned paths will all start with dir. In particular, if dir
    is absolute, then all returned paths will be absolute.

    If hidden=True, include hidden files and files inside hidden
    directories."""
    for root, dirs, files in walk(dir):
        if not hidden:
            dirs = filter_hidden(dirs)
            files = filter_hidden(files)
        for f in files:
            yield os.path.join(root, f)

class DestinationFinder(object):
    """A class for converting source paths to destination paths."""
    def __init__(self, src_dir, dest_dir, src_exts, dest_ext, hidden=False):
        self.src_dir = os.path.realpath(src_dir)
        self.dest_dir = os.path.realpath(dest_dir)
        # These need leading dots
        self.src_exts = src_exts
        self.dest_ext = dest_ext
        self.include_hidden = hidden

    def find_dest(self, src):
        """Returns the absolute destination path for a source path.

        If the source path is absolute, it must lie inside src_dir, or
        ValueError is thrown. A non-absolute path is assumed to be
        relative to src_dir."""
        # Make src relative
        if os.path.isabs(src):
            if not is_subpath(src, self.src_dir):
                raise ValueError("Absolute path must fall within src_dir")
            src = os.path.relpath(src, self.src_dir)
        base, ext = splitext_afterdot(src)
        if ext.lower() in self.src_exts:
            dest_relpath = base + self.dest_ext
        else:
            dest_relpath = src
        return os.path.join(self.dest_dir, dest_relpath)

    def walk_source_files(self):
        """An iterator over all files in the source directory."""
        return walk_files(self.src_dir, hidden=self.include_hidden)

    def walk_target_files(self):
        """An iterator over all files that are to be created in the destination directory."""
        return map(self.find_dest, self.walk_source_files())

    def walk_source_target_pairs(self):
        """iter(zip(self.walk_source_files(), self.walk_target_files()))'.

        Only it's more efficient."""
        return ((src, self.find_dest(src)) for src in self.walk_source_files())

    def walk_existing_dest_files(self):
        """An iterator over all existing files in the destination directory."""
        return walk_files(self.dest_dir, hidden=self.include_hidden)

    def walk_extra_dest_files(self):
        """An iterator over all existing files in the destination directory that are not targets of source files.

        These are the files that transfercoder would delete if given the --delete option."""
        return set(self.walk_existing_dest_files()).difference(self.walk_target_files())

    def transfercodes(self, eopts=None):
        """Generate Transfercode objects for all src files as a list."""
        return list(Transfercode(src,dest) for src, dest in self.walk_source_target_pairs())

def create_dirs(dirs):
    """Ensure that a list of directories all exist"""
    for d in dirs:
        if not os.path.isdir(d):
            logging.debug("Creating directory: %s", d)
            os.makedirs(d)

def comma_delimited_set(alist):
    # Handles stripping spaces and eliminating zero-length items
    items = [x.strip() for x in alist.split(",")]
    return set([x for x in items if x])

def positive_int(value):
    ivalue = int(value)
    if ivalue <= 0:
         raise argparse.ArgumentTypeError("%r is not a positive int value" % value)
    return ivalue

def directory(x):
    """Resolve symlinks, then return the result if it is a directory.

    Otherwise throw an error."""
    path = os.path.realpath(x)
    if not os.path.isdir(path):
        if path == x:
            msg = "Not a directory: %s" % x
        else:
            msg = "Not a directory: %s -> %s" % (x, path)
        raise argparse.ArgumentTypeError(msg)
    else:
        return path

def potential_directory(x):
    return directory(x) if os.path.exists(x) else x

default_transcode_formats = set(("flac", "wv", "wav", "ape", "fla"))

def parse_options():
    parser = argparse.ArgumentParser(description='Mirror a directory with transcoding.')
    parser.add_argument('-j', '--jobs', action='store', type=positive_int, default=default_job_count(), help="Number of transcoding jobs to run in parallel. Transfers will always run sequentially. The default is the number of cores available on the system. A value of 1 will run transcoding in parallel with copying. Use -j0 to force full sequential operation.")
    parser.add_argument('-n', '--dry-run', action='store_true', default=False, help="Don't actually modify anything.")
    parser.add_argument('-f', '--force', action='store_true', help='Update destination files even if they are newer.')
    parser.add_argument('-i', '--transcode_formats', action='store', type=comma_delimited_set, help="A comma-separated list of input file extensions that must be transcoded.", default=','.join(default_transcode_formats))
    parser.add_argument('-o', '--target-format', action='store', help="All input transcode formats will be transcoded to this output format.", default='ogg')
    parser.add_argument('-E', '--extra-encoder-options', action='store', help="Extra options to pass to the encoder. This is passed to pacpl using the '--eopts' option. If you think you need to use this, you should probably just edit pacpl's config file instead.")
    parser.add_argument('-z', '--include-hidden', action='store', help="Don't skip directories and files starting with a dot.")
    parser.add_argument('-D', '--delete', action='store_true', help="Delete files in the destination that do not have a corresponding file in the source directory.")
    parser.add_argument('-q', '--quiet', action='store_true', default=False, help="Do not print informational messages.")
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help="Print debug messages that are probably only useful if something is going wrong.")
    parser.add_argument('source_directory', type=directory, help="The directory with all your music in it.")
    parser.add_argument('destination_directory', type=potential_directory, help="The directory where output files will go. The directory hierarchy of the source directory will be replicated here.")
    return parser.parse_args()


GLOBAL_FORCE=False
GLOBAL_DRY_RUN=False
def init_transfer(force, dry_run):
    """Helper function to initialize the per-process Pool options."""
    global GLOBAL_FORCE, GLOBAL_DRY_RUN
    GLOBAL_FORCE=force
    GLOBAL_DRY_RUN=dry_run


def start_transfer(tfc):
    """Helper function to start a transfer."""
    return tfc.transfer(force=GLOBAL_FORCE, dry_run=GLOBAL_DRY_RUN)


def configure_logging(quiet, verbose):
    """Set log format with logging.basicConfig()"""
    if quiet:
        level=logging.WARN
    elif verbose:
        level=logging.DEBUG
    else:
        level=logging.INFO
    format = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(level=level, format=format)


def main(source_directory, destination_directory,
         transcode_formats=default_transcode_formats,
         target_format="ogg",
         extra_encoder_options="",
         dry_run=False, include_hidden=False, delete=False, force=False,
         quiet=False, verbose=False,
         jobs=default_job_count()):
    """Mirror a directory with transcoding.

    Everything in the source directory is copied to the destination,
    except that any files of the specified transcode formats are
    transcoded into the target format use Perl Audio Converter. All other
    files are copied over unchanged.

    The default behavior is to transcode several lossless formats
    (flac, wavpack, wav, and ape) to ogg, and all other files are
    copied over unmodified."""
    configure_logging(quiet, verbose)
    logging.debug("source_directory=%s", source_directory)
    logging.debug("destination_directory=%s", destination_directory)
    logging.debug("transcode_formats=%s, target_format=%s, extra_encoder_options=%s",
        transcode_formats, target_format, extra_encoder_options)
    logging.debug("dry_run=%s, include_hidden=%s, delete=%s, force=%s",
        dry_run, include_hidden, delete, force)
    logging.debug("quiet=%s, verbose=%s, jobs=%s",
        quiet, verbose, jobs)

    if target_format in transcode_formats:
        logging.error('The target format %s must not be one of the transcode formats', target_format)
        return 1

    if dry_run:
        logging.info("Running in --dry_run mode. Nothing actually happens.")

    source_directory = os.path.realpath(source_directory)
    destination_directory = os.path.realpath(destination_directory)
    df = DestinationFinder(source_directory, destination_directory,
                           transcode_formats, target_format, include_hidden)
    logging.info("Getting transfer objects for each file from %s", source_directory)
    transfercodes = df.transfercodes(eopts=extra_encoder_options)
    num_tasks = len(transfercodes)

    errors = 0
    if not dry_run:
        create_dirs(set(x.dest_dir for x in transfercodes))

    logging.info("Running %d %s in parallel to transcode and transfer %d files",
        jobs, ("jobs" if jobs > 1 else "job"), num_tasks)
    # start jobs as separate processses
    transcode_pool = multiprocessing.Pool(jobs, init_transfer, (force, dry_run))
    try:
        # the order of transferred files is not important, so use imap_unordered
        results = transcode_pool.imap_unordered(start_transfer, transfercodes)
        transcode_pool.close()
        transcode_pool.join()
        for i, result in enumerate(results, start=1):
            errors += result
            logging.debug('done {0:%}'.format(i/num_tasks))
    except KeyboardInterrupt:
        logging.warning("Stopping jobs....")
        transcode_pool.terminate()
        logging.warning("... stopped")
    if delete:
        # Remove files that are in destination folders but not in the source
        for f in df.walk_extra_dest_files():
            logging.info("Deleting: %s", f)
            if not dry_run:
                os.remove(f)

    logging.info("Done with %d %s", errors, ("error" if errors == 1 else "errors"))
    if dry_run:
        logging.info("Ran in --dry_run mode. Nothing actually happened.")
    return 1 if errors > 0 else 0


if __name__ == "__main__":
    options = parse_options()
    res = main(options.source_directory, options.destination_directory,
         transcode_formats=options.transcode_formats,
         target_format=options.target_format,
         extra_encoder_options=options.extra_encoder_options,
         dry_run=options.dry_run,
         include_hidden=options.include_hidden,
         delete=options.delete, force=options.force,
         quiet=options.quiet, verbose=options.verbose,
         jobs=options.jobs)
    sys.exit(res)
