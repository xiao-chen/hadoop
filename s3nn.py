#!/usr/bin/env python3
#
# Author: Lei Xu <eddyxu@gmail.com>

import boto
from dateutil import parser
from boto.s3.key import Key
import os
import argparse
from xml.etree import ElementTree as et


BUCKET = 'eddy-hack'

s3 = boto.connect_s3()
bucket = s3.get_bucket(BUCKET)


def create_bucket(args):
    for i in range(args.n):
        path = "/foo/file-{}".format(i)
        k = Key(bucket)
        k.key = path
        k.set_contents_from_string('{}'.format(i))
        if i % 100 == 0:
            print("Created {} files".format(i))


class FsImageBuilder(object):
    ROOT_ID = 2 ** 14

    def __init__(self):
        self.tree = et.ElementTree()
        self.fsimage = et.Element('fsimage')
        self.tree._setroot(self.fsimage)
        self.inode_map = {'/': {
                'inode': self.ROOT_ID,
                'is_dir': True,
                'size': 0,
                'mtime': '2016-04-28T18:33:49.000Z',
            }}
        self.last_inode = self.ROOT_ID + 1
        self.last_block_id = 1024 ** 3 + 1 # SequentialBlockIdGenerator.java
        self.ns = {self.ROOT_ID: set([])}
        self.last_genstamp = 1001

    def element(self, name, text):
        elem = et.Element(name)
        elem.text = str(text)
        return elem

    def version(self):
        ver = et.Element('version')
        ver.append(self.element('layoutVersion', '-64'))
        ver.append(self.element('onDiskVersion', '1'))
        ver.append(self.element('oivRevision',
                                'e0cb426758b3d716ff143f723fc16ef2f1e4971b'))
        self.fsimage.append(ver)

    @staticmethod
    def get_path_components(path):
        return os.path.normpath(path).split(os.sep)

    def assign_inode(self, meta):
        path, size, mtime = meta
        components = self.get_path_components(path)
        last_path = "/"
        for idx in range(1, len(components)):
            comp = components[idx]
            cur_path = os.path.join(last_path, comp)
            is_dir = idx != len(components) - 1
            if cur_path not in self.inode_map:
                inode = self.last_inode
                self.last_inode += 1
                self.inode_map[cur_path] = {
                    "inode": inode,
                    'is_dir': is_dir,
                    'size': 0 if is_dir else size,
                    'mtime': mtime
                }
                parent_inode = self.inode_map[last_path]['inode']
                self.ns[parent_inode].add(inode)
                if inode != self.ROOT_ID:
                    assert inode not in self.ns
                self.ns[inode] = set([])
            last_path = cur_path

    def name_section(self):
        name = et.Element('NameSection')
        name.append(self.element('namespaceId', 24070276))
        name.append(self.element('genstampV1', 1000))
        name.append(self.element('genstampV2', 1000))
        name.append(self.element('genstampV1Limit', 0))
        name.append(self.element('lastAllocatedBlockId', 1073741824))
        name.append(self.element('txid', 20))
        self.fsimage.append(name)

    def inode_element(self, path, meta):
        elem = et.Element('inode')
        elem.append(self.element('id', meta['inode']))
        is_dir = meta['is_dir']
        if meta['is_dir']:
            type_str = 'DIRECTORY'
        else:
            type_str = 'FILE'
        elem.append(self.element('name', os.path.split(path)[1]))
        elem.append(self.element('type', type_str))
        ts = int(parser.parse(meta['mtime']).timestamp() * 1000)
        elem.append(self.element('mtime', ts))

        if is_dir:
            elem.append(self.element('permission', 'xiao:supergroup:0755'))
            elem.append(self.element('nsquota', -1))
            elem.append(self.element('dsquota', -1))
        else:
            elem.append(self.element('atime', ts))
            elem.append(self.element('permission', 'xiao:supergroup:0644'))
            elem.append(self.element('preferredBlockSize', 134217728))
            elem.append(self.element('replication', 1))
            elem.append(self.element('storagePolicyId', 5))  # PROVIDED

            blocks = et.Element('blocks')
            block = et.Element('block')
            block.append(self.element('genstamp', self.last_genstamp))
            self.last_genstamp += 1
            block.append(self.element('numBytes', meta['size']))
            block.append(self.element('id', self.last_block_id))
            self.last_block_id += 1
            blocks.append(block)
            elem.append(blocks)
            
        return elem

    def inode_section(self):
        inode = et.Element('INodeSection')
        inode.append(self.element('lastInodeId', self.last_inode))
        inode.append(self.element('numInodes', len(self.inode_map)))
        print(sorted(self.inode_map.keys()))
        for k, v in self.inode_map.items():
            inode.append(self.inode_element(k, v))

        self.fsimage.append(inode)

    def other_sections(self):
        self.fsimage.append(
            self.element('INodeReferenceSection', '')
        )
        snapshot = et.Element('SnapshotSection')
        snapshot.append(self.element('snapshotCounter', 0))
        snapshot.append(self.element('numSnapshots', 0))
        self.fsimage.append(snapshot)

        self.fsimage.append(
            et.Element('FileUnderConstructionSection')
        )

        snapshot_diff = et.Element('SnapshotDiffSection')
        diff_entry = et.Element('dirDiffEntry')
        diff_entry.append(self.element('inodeId', 16385))
        diff_entry.append(self.element('count', 0))
        snapshot_diff.append(diff_entry)
        self.fsimage.append(snapshot_diff)

        secret = et.Element('SecretManagerSection')
        secret.append(self.element('currentId', 0))
        secret.append(self.element('tokenSequenceNumber', 0))
        secret.append(self.element('numDelegationKeys', 0))
        secret.append(self.element('numTokens', 0))
        self.fsimage.append(secret)

        cache = et.Element('CacheManagerSection')
        cache.append(self.element('nextDirectiveId', 1))
        cache.append(self.element('numDirectives', 0))
        cache.append(self.element('numPools', 0))
        self.fsimage.append(cache)

    def dir_section(self):
        dir_sec = et.Element('INodeDirectorySection')
        for k, v in self.ns.items():
            if v:
                dir_elem = et.Element('directory')
                dir_elem.append(self.element('parent', k))
                for c in v:
                    dir_elem.append(self.element('child', c))
                dir_sec.append(dir_elem)
        self.fsimage.append(dir_sec)

    def files(self, files):
        for f in files:
            self.assign_inode(f)
        self.name_section()
        self.inode_section()
        self.dir_section()
        self.other_sections()

    def write(self, fpath):
        self.tree.write(fpath, xml_declaration=True,
                        short_empty_elements=False)


def create_fsimage(args, files):
    builder = FsImageBuilder()
    builder.version()
    builder.files(files)
    builder.write('fsimage.xml')


def list_bucket(args):
    files = []
    for i in bucket.list():
        s3_file = ('/' + i.key, i.size, i.last_modified)
        files.append(s3_file)
    files = sorted(files, key=lambda f: f[0])
    create_fsimage(args, files)


def main():
    """Main function of ....
    """
    parser = argparse.ArgumentParser()
    sub_parsers = parser.add_subparsers()

    create_parser = sub_parsers.add_parser('create', help='create data')
    create_parser.add_argument('-n', metavar='NUM', type=int, default=1000,
                               help='total number of files')
    create_parser.set_defaults(func=create_bucket)

    list_parser = sub_parsers.add_parser('list', help='list bucket')
    list_parser.set_defaults(func=list_bucket)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
