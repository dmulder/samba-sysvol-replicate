#!/usr/bin/python
from samba import smb, getopt
import optparse, tdb, os.path, sys, argparse
from ConfigParser import ConfigParser
from StringIO import StringIO

def win_path_join(a, b):
    return '\\'.join([a, b])

def download_files(conn, remote_path, local_path):
    # Ensure the directory exists
    if not os.path.exists(local_path):
        os.makedirs(local_path)
    if not os.path.isdir(local_path):
        raise IOError('Path \'%s\' is not a directory' % local_path)
    ls = conn.list(remote_path)
    for f in ls:
        if f['attrib'] & 0x00000010: # MS-FSCC FILE_ATTRIBUTE_DIRECTORY
            download_files(conn, win_path_join(remote_path, f['name']), os.path.join(local_path, f['name']))
        else:
            # Download the file
            fname = os.path.join(local_path, f['name'])
            rname = win_path_join(remote_path, f['name'])
            open(fname, 'w').write(conn.loadfile(rname))

if __name__ == '__main__':
    parse = argparse.ArgumentParser(description='Sysvol replication tool')
    parse.add_argument('server', help='FQDN of server to replicate sysvol from')
    args = parse.parse_args()

    parser = optparse.OptionParser()
    lp = getopt.SambaOptions(parser).get_loadparm()
    creds = getopt.CredentialsOptions(parser).get_credentials(lp, fallback_machine=True)
    conn = smb.SMB(args.server, 'sysvol', lp=lp, creds=creds)

    gpo_versions = '%s/%s' % (lp.get('path', 'sysvol'), 'versions.tdb')
    if os.path.isfile(gpo_versions):
        vers_f = tdb.open(gpo_versions)
    else:
        vers_f = tdb.Tdb(gpo_versions, 0, tdb.DEFAULT, os.O_CREAT|os.O_RDWR)

    gpo_path = win_path_join(lp.get('realm').lower(), 'Policies')
    gpos = [x['name'] for x in conn.list(gpo_path)]
    for gpo in gpos:
        s_gpo_path = win_path_join(gpo_path, gpo)
        # Check if the version has changed
        fname = win_path_join(s_gpo_path, 'GPT.INI')
        data = conn.loadfile(fname)
        gpt_ini = ConfigParser()
        gpt_ini.readfp(StringIO(data))
        new_vers = gpt_ini.get('General', 'Version')
        old_vers = vers_f.get(gpo)
        if (old_vers and old_vers != new_vers) or not old_vers:
            vers_f.transaction_start()
            vers_f.store(gpo, new_vers)
            # Re-download the gpo
            download_files(conn, s_gpo_path, os.path.join(lp.get('path', 'sysvol'), s_gpo_path.replace('\\', '/')))
            vers_f.transaction_commit()
    vers_f.close()
