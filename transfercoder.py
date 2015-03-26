#!/usr/bin/env python

import os
import os.path
from subprocess import call
import shutil
from warnings import warn
import sys
import re
import UserDict
import argparse
from itertools import imap
import quodlibet.config
quodlibet.config.init()
from quodlibet.formats import MusicFile
import multiprocessing
import logging
from multiprocessing import Pool
from distutils.spawn import find_executable

rsync_exe = find_executable("rsync")
pacpl_exe = find_executable("pacpl")

def default_job_count():
    try:
        return multiprocessing.cpu_count()
    except:
        return 1

def call_silent(cmd, *args, **kwargs):
    """Like subprocess.call, but redirects stdin/out/err to null device."""
    nullsrc = open(os.devnull, "r")
    nullsink = open(os.devnull, "w")
    logging.debug("Calling command: %s", repr(cmd))
    return call(cmd, *args, stdin=nullsrc, stdout=nullsink, stderr=nullsink, **kwargs)

def test_executable(exe, options=("--help",)):
    """Test whether exe can be executed by doing `exe --help` or similar.

    Returns True if the command succeeds, false otherwise. The exe's
    stdin, stdout, and stderr are all redirected from/to the null
    device."""
    cmd = (exe, ) + options
    try:
        return call_silent(cmd) == 0
    except:
        return False

def filter_hidden(paths):
    return filter(lambda x: x[0] != ".", paths)

def splitext_afterdot(path):
    """Same as os.path.splitext, but the dot goes to the base."""
    base, ext = os.path.splitext(path)
    if len(ext) > 0 and ext[0] == ".":
        base += "."
        ext = ext[1:]
    return (base, ext)

class AudioFile(UserDict.DictMixin):
    """A simple class just for tag editing.

    No internal mutagen tags are exposed, or filenames or anything. So
    calling clear() won't destroy the filename field or things like
    that. Use it like a dict, then .write() it to commit the changes.

    Optional argument blacklist is a list of regexps matching
    non-transferrable tags. They will effectively be hidden, nether
    settable nor gettable.

    Or grab the actual underlying quodlibet format object from the
    .data field and get your hands dirty."""

    def __init__(self, filename, blacklist=()):
        self.data = MusicFile(filename)
        # Also exclude mutagen's internal tags
        self.blacklist = [ re.compile("^~") ] + blacklist

    def __getitem__(self, item):
        if self.blacklisted(item):
            warn("%s is a blacklisted key." % item)
        else:
            return self.data.__getitem__(item)

    def __setitem__(self, item, value):
        if self.blacklisted(item):
            warn("%s is a blacklisted key." % item)
        else:
            return self.data.__setitem__(item, value)

    def __delitem__(self, item):
        if self.blacklisted(item):
            warn("%s is a blacklisted key." % item)
        else:
            return self.data.__delitem__(item)

    def blacklisted(self, item):
        """Return True if tag is blacklisted.

        Blacklist automatically includes internal mutagen tags (those
        beginning with a tilde)."""
        for regex in self.blacklist:
            if re.search(regex, item):
                return True
        else:
            return False

    def keys(self):
        return [ key for key in self.data.keys() if not self.blacklisted(key) ]

    def write(self):
        return self.data.write()

# A list of regexps matching non-transferrable tags, like file format
# info and replaygain info. This will not be transferred from source,
# nor deleted from destination.
blacklist_regexes = [ re.compile(s) for s in (
        'encoded',
        'replaygain',
        ) ]

def copy_tags (src, dest):
    """Replace tags of dest file with those of src.

Excludes format-specific tags and replaygain info, which does not
carry across formats."""
    m_src = AudioFile(src, blacklist = blacklist_regexes)
    m_dest = AudioFile(dest, blacklist = m_src.blacklist)
    m_dest.clear()
    m_dest.update(m_src)
    m_dest.write()

def copy_mode(src, dest):
    """Copy file mode. Is allowed to fail since some network filesystems
    (eg. CIFS) don't allow those mode settings."""
    try:
        shutil.copymode(src, dest)
    except OSError:
        # It's ok if setting the mode fails
        pass

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
        if dry_run:
            return
        # pacpl expects a relative path with no extension, apparently
        rel_dest = os.path.relpath(self.dest, self.src_dir)
        rel_dest_base = os.path.splitext(rel_dest)[0]
        command = [pacpl_exe] + (["--eopts", self.eopts] if self.eopts else []) + \
          ["--overwrite", "--keep", "--to", self.dest_ext, "--outfile", rel_dest_base, self.src]
        if call_silent(command) != 0:
            raise Exception("Perl Audio Converter failed")
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
                retval = call_silent([rsync_exe, "-q", "-p", self.src, self.dest])
                success = (retval == 0)
            except:
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
            except:
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
    for root, dirs, files in os.walk(dir):
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
        return imap(self.find_dest, self.walk_source_files())

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
        """Generate Transfercode objects for all src files.

        Optional arg 'eopts' is passed to the Transfercode() constructor."""
        return (Transfercode(src,dest) for src, dest in self.walk_source_target_pairs())

def create_dirs(dirs):
    """Ensure that a list of directories all exist"""
    for d in dirs:
        if not os.path.isdir(d):
            logging.debug("Creating directory: %s", d)
            os.makedirs(d)

def comma_delimited_set(x):
    # Handles stripping spaces and eliminating zero-length items
    return set(filter(len, list(x.strip() for x in x.split(","))))

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
    if quiet:
        level=logging.WARN
    elif verbose:
        level=logging.DEBUG
    else:
        level=logging.INFO
    format = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(level=level, format=format)

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

    def start_transfer(tfc):
        """Helper function to start a transfer."""
        return tfc.transfer(force=force, dry_run=dry_run)

    errors = 0
    if not dry_run:
        create_dirs(set(x.dest_dir for x in transfercodes))

    logging.info("Running %s %s in parallel to transcode and transfer files", jobs, ("jobs" if jobs > 1 else "job"))
    transcode_pool = Pool(jobs)
    try:
        results = transcode_pool.imap_unordered(start_transfer, transfercodes)
        transcode_pool.close()
        transcode_pool.join()
        for result in results:
            errors += result
    except KeyboardInterrupt:
        logging.warning("Stopping jobs....")
        transcode_pool.terminate()
        logging.warning("... stopped")
    if delete:
        for f in df.walk_extra_dest_files():
            logging.info("Deleting: %s", f)
            if not dry_run:
                os.remove(f)

    logging.info("Done with %d %s", errors, ("error" if errors == 1 else "errors"))
    if dry_run:
        logging.info("Ran in --dry_run mode. Nothing actually happened.")
    return errors


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
