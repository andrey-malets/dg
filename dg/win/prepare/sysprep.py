import logging
import os


def copy_setup_scripts(ssh_client, scripts):
    dest_dir = '/cygdrive/c/Windows/Setup/Scripts'
    ssh_client.ssh(['mkdir', '-p', dest_dir])
    for script in scripts:
        dest_file = '{}/{}'.format(dest_dir, os.path.basename(script))
        logging.info('copying setup script {} to {}'.format(script, dest_file))
        ssh_client.scp(script, dest_file)


def start_sysprep(ssh_client, sysprep_xml, timeout):
    logging.info('Starting sysprep with {}'.format(sysprep_xml))
    sysprep = r'C:\\Windows\\system32\\sysprep\\sysprep.exe'
    cygwin_path = '/cygdrive/c/Users/{}/sysprep.xml'.format(ssh_client.login)
    windows_path = r'C:\\Users\\{}\\sysprep.xml'.format(ssh_client.login)
    ssh_client.scp(sysprep_xml, cygwin_path)
    ssh_client.ssh(['cmd', '/c', 'start', '/w',
                    sysprep, '/oobe', '/generalize', '/shutdown',
                    '/unattend:{}'.format(windows_path)], timeout=timeout)
