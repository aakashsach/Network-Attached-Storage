from basic_defs import NAS
from cloud import RAID_on_Cloud, AWS_S3, Azure_Blob_Storage, Google_Cloud_Storage
from hexdump import hexdump

import argparse
import traceback
import os
import sys
import string

class local_NAS(NAS):
    def __init__(self):
        self.fds = dict()

    def open(self, filename):
        path = os.path.join(os.getcwd(), "tmp", filename)
        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
        realfd = os.open(path, os.O_RDWR | os.O_CREAT)
        newfd = None
        for fd in range(256):
            if fd not in self.fds:
                newfd = fd
                break
        if newfd is None:
            raise IOError("Opened files exceed system limitation.")
        self.fds[newfd] = realfd
        return newfd

    def read(self, fd, len, offset):
        if fd not in self.fds:
            raise IOError("File descriptor %d does not exist." % fd)
        realfd = self.fds[fd]
        os.lseek(realfd, offset, os.SEEK_SET)
        return os.read(realfd, len)

    def write(self, fd, data, offset):
        if fd not in self.fds:
            raise IOError("File descriptor %d does not exist." % fd)
        realfd = self.fds[fd]
        os.lseek(realfd, offset, os.SEEK_SET)
        os.write(realfd, data.decode('utf-8'))

    def close(self, fd):
        if fd not in self.fds:
            raise IOError("File descriptor %d does not exist." % fd)
        os.close(self.fds[fd])
        del self.fds[fd]
        return

    def delete(self, filename):
        path = os.path.join(os.getcwd(), "tmp", filename)
        os.unlink(path)
        path = os.path.dirname(path)
        while not os.listdir(path):
            os.rmdir(path)
            path = os.path.dirname(path)

    def get_storage_sizes(self):
        return 0

def usage():
    print("***********************************************")
    print("**     Welcome to the RAID-on-Cloud NAS!     **")
    print("**                                           **")
    print("** Type one of the following cmds:           **")
    print("**     o  / open   <filename>                **")
    print("**     r  / read   <fd> <len> <offset>       **")
    print("**     rb / readb  <fd> <len> <offset>       **")
    print("**     w  / write  <fd> <offset>             **")
    print("**       (input from the next line)          **")
    print("**     wb / writeb <fd> <offset>             **")
    print("**       (input from the next line)          **")
    print("**     c  / close  <fd>                      **")
    print("**     d  / delete <filename>                **")
    print("**     q  / quit                             **")
    print("***********************************************")

def main():
    cmd_parser = argparse.ArgumentParser(description="RAID-on-Cloud NAS CLI program.")
    cmd_parser.add_argument('--local', '-l', action='store_true', help="Running NAS in local mode (without cloud backends)") 
    args = cmd_parser.parse_args()

    if args.local:
        nas = local_NAS()
    else:
        nas = RAID_on_Cloud()

    cli_parser = argparse.ArgumentParser()
    cli_parser.add_argument('cmd', choices=[
        'open',    'o', 
        'read',    'r', 
        'readb',   'rb', 
        'write',   'w', 
        'writeb',  'wb', 
        'close',   'c', 
        'delete',  'd', 
        'quit',    'q'])
    cli_parser.add_argument('rest', nargs=argparse.REMAINDER)

    usage()
    while True:
        astr = raw_input('NAS> ')
        # print astr
        try:
            args = cli_parser.parse_args(astr.split())

            if args.cmd == 'open' or args.cmd == 'o':
                if len(args.rest) != 1:
                    raise SystemExit
                fd = nas.open(args.rest[0])
                print("Opened file descriptor %d for %s" % (fd, args.rest[0]))
                continue

            if args.cmd == 'read' or args.cmd == 'r' or args.cmd == 'readb' or args.cmd == 'rb':
                if len(args.rest) != 3:
                    raise SystemExit
                try:
                    fd = int(args.rest[0])
                    offset = int(args.rest[1])
                    size = int(args.rest[2])
                except ValueError:
                    raise SystemExit

                data = nas.read(fd, offset, size)
                if args.cmd == 'read' or args.cmd == 'r':
                    data = data.decode('utf-8')
                    if not all(c in string.printable for c in data):
                        print("Output contains unprintable characters. Use rb / readb instead.")
                        continue
                    sys.stdout.write(data + '<eof>\n')
                else:
                    sys.stdout.write(hexdump(data))
                continue

            if args.cmd == 'write' or args.cmd == 'w' or args.cmd == 'writeb' or args.cmd == 'wb':
                if len(args.rest) != 2:
                    raise SystemExit
                try:
                    fd = int(args.rest[0])
                    offset = int(args.rest[1])
                except ValueError:
                    raise SystemExit
 
                print("Enter the content to write (To break, type <ENTER> and then <Control-D>):")
                if args.cmd == 'write' or args.cmd == 'w':
                    data = sys.stdin.read().encode('utf-8')
                    if data[-1] == ord('\n'):
                        data = data[:-1]
                else:
                    data = ""
                    for l in sys.stdin.readlines():
                        for d in l.split():
                            data += d.decode('hex')
                nas.write(fd, data, offset)
                continue
 
 
            if args.cmd == 'close' or args.cmd == 'c':
                if len(args.rest) != 1:
                    raise SystemExit
                try:
                    fd = int(args.rest[0])
                except ValueError:
                    raise SystemExit
                nas.close(fd)
                print("File descriptor %d closed." % fd)
                continue

            if args.cmd == 'delete' or args.cmd == 'd':
                if len(args.rest) != 1:
                    raise SystemExit
                nas.delete(args.rest[0])
                print("File %s deleted." % args.rest[0])
                continue

            if args.cmd == 'quit' or args.cmd == 'q':
                print("Goodbye!!!")
                break

        except NotImplementedError as e:
            print("Function not implemented.")
            usage()
            continue

        except SystemExit:
            # trap argparse error message
            print("Error.")
            usage()
            continue

        except Exception, e:
            traceback.print_exc()
            usage()
            continue


if __name__ == "__main__":
        main()
