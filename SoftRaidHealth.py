
import socket
import os
import sys
import datetime
import re
import sqlite3
import subprocess

DEBUG = True;

HOSTNAME = socket.gethostname()
WORKDIR = '/home/snell/Projets/CronJobs/softraid/'
EXEC_TIME = datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')

EMAIL_SMTP = "SMTP.DOMAIN.COM"
EMAIL_TO = "ME@DOMAIN.COM"
EMAIL_SUBJECT = "%s RAID Health : %s at %s"%(HOSTNAME, 'status', EXEC_TIME)
EMAIL_FROM = HOSTNAME
ERROR_LEVEL = {0: 'OK', 1: 'Warning', 2: 'Problem', 3: 'Degraded', 4: 'Failure'}
STATUS_LEVEL = {0: 'OK', 1: 'Warning', 2: 'Problem', 3: 'Degraded', 4: 'Failure'}


class System(object):

    id = None

    def __init__(self, hostname, timeid, conn):
        self.conn = conn
        self.timeid = timeid
        self.hostname = hostname
        self.status = None
        self.md_devices = self.set_md_devices()

    def set_md_devices(self):
        mdstat = subprocess.Popen(["cat", "/proc/mdstat"], stdout=subprocess.PIPE).communicate()[0]
        md_regex = re.compile(r"md[0-9]{0,3}.*?.(U]|_])", re.DOTALL | re.IGNORECASE)
        md_devices = []
        if md_regex.finditer(mdstat):
            for md_string in md_regex.finditer(mdstat):
                md = MdDevice(md_string.group(0), self.timeid, self.conn)
                md_devices.append(md)
        return md_devices

#    def set_md_devices(self):
#        md_devices = []
#        md = MdDevice('md1', self.timeid)
#        md_devices.append(md)
#        return md_devices

    def save(self):
        cur = self.conn.cursor()
        try:
            cur.execute('select rowid from systems WHERE hostname = ? and timeid = ?', (self.hostname, EXEC_TIME))
            rowid = cur.fetchone()
            if rowid:
                self.id = rowid[0]
            else:
                cur.execute('insert into systems(hostname, timeid) values(?,?)', (self.hostname, EXEC_TIME))
                self.id = cur.lastrowid
                conn.commit()
        finally:
            cur.close()

        for md_dev in self.md_devices:
            md_dev.save(self.id)

        return self.id

class MdDevice(object):

    id = None
    health = None
    devices = None

    def __init__(self, string, timeid, conn):
        self.conn = conn
        self.timeid = timeid
        self.string = string
        self.name = self.set_name(string)
        self.health = self.set_health(string)
        self.status = self.set_status()
        self.level = self.set_level()
        self.devices = self.set_devices(string)

    def set_name(self, string):
        id_regex = re.compile(r"md[0-9]{0,3}", re.IGNORECASE)
        for id in id_regex.finditer(string):
            return id.group(0)

    def set_health(self, string):
        if self.health:
            return self.health
        else:
            health = ERROR_LEVEL[0]
            for device in self.set_devices(string):
                if device.health > health:
                   health = device.health
            return health

    def set_status(self):
        return 0

    def set_level(self):
        return 0

    def set_devices(self, string):
        if self.devices:
            return self.devices
        else:
            dev_regex = re.compile(r"sd[a-z]{1,2}[0-9]{1,2}\[[0-9{1-2}]\]", re.IGNORECASE)
            sd_dev = []
            for sd_string in dev_regex.finditer(string):
                sd_dev.append(Device(sd_string.group(0), self.timeid, self.conn))
            return sd_dev

    def save(self, parent_id):
        cur = self.conn.cursor()
        try:
            cur.execute('select rowid from md_devices WHERE system_id = ? and name = ?', (parent_id, self.name))
            rowid = cur.fetchone()
            if rowid:
                self.id = rowid[0]
            else:
                cur.execute('insert into md_devices(system_id, name, status, level, health) values(?,?,?,?,?)',
                            (parent_id, self.name, self.status, self.level, self.health))
                self.id = cur.lastrowid
                conn.commit()
        finally:
            cur.close()

        for sd_dev in self.devices:
            sd_dev.save(self.id)

        return self.id

class Device(object):

    smart_output = None
    id = None
    smart_attributes = []

    def __init__(self, string, timeid, conn):
        self.conn = conn
        self.timeid = timeid
        self.string = string
        self.name = self.set_name(string)
        self.dev_name = self.set_dev_name(string)
        self.sd_name = self.set_sd_name(string)
        self.model = self.set_model()
        self.serial = self.set_serial()
        self.firmware = self.set_firmware()
        self.status = self.set_status()
        self.health = self.set_health()

    def set_name(self, string):
        id_regex = re.compile(r"\[[0-9]\]{1,3}", re.IGNORECASE)
        for id in id_regex.finditer(string):
            return id.group(0).strip("[").strip("]")

    def set_sd_name(self, string):
        sd_regex = re.compile(r"^sd[a-z]{1,2}", re.IGNORECASE)
        for sd_name in sd_regex.finditer(string):
            return sd_name.group(0).strip("[").strip("]")

    def set_dev_name(self, string):
        dev_regex = re.compile(r"^sd[a-z]{1,2}[0-9]{1,2}", re.IGNORECASE)
        for dev_name in dev_regex.finditer(string):
            return dev_name.group(0).strip("[").strip("]")

    def set_model(self):
        model_regex = re.compile(r"Device Model.*", re.IGNORECASE)
        for model in model_regex.finditer(self.get_smart_output()):
            return model.group(0).split(':')[1].strip()

    def set_serial(self):
        serial_regex = re.compile(r"Serial Number.*", re.IGNORECASE)
        for serial in serial_regex.finditer(self.get_smart_output()):
            return serial.group(0).split(':')[1].strip()

    def set_firmware(self):
        firmware_regex = re.compile(r"Firmware Version.*", re.IGNORECASE)
        for firmware in firmware_regex.finditer(self.get_smart_output()):
            return firmware.group(0).split(':')[1].strip()

    def set_status(self):
        return 0

    def set_health(self):
        return 0

    def get_smart_output(self):
        if self.smart_output:
            return self.smart_output
        else:
            self.smart_output = subprocess.Popen(["smartctl", "-iA", "/dev/%s" % self.sd_name], stdout=subprocess.PIPE).communicate()[0]
            return self.smart_output

    def get_smart_attributes(self):
        if self.smart_attributes:
            return self.smart_attributes
        else:
            self.smart_attributes = []
            attributes_regex = re.compile(r"^[\s]{0,3}[0-9]{1,3}.*[\n]", re.MULTILINE)
            for attribute_line in attributes_regex.finditer(self.get_smart_output()):
                self.smart_attributes.append(SmartAttribute(attribute_line.group(0).strip(), self.conn))
            return self.smart_attributes

    def save(self, parent_id):
        cur = self.conn.cursor()
        try:
            cur.execute('select rowid from sd_devices WHERE md_device_id = ? and name = ?', (parent_id, self.name))
            rowid = cur.fetchone()
            if rowid:
                self.id = rowid[0]
            else:
                cur.execute('insert into sd_devices(md_device_id, name, sd_name, dev_name, status, health, model, serial, firmware) values(?,?,?,?,?,?,?,?,?)',
                            (parent_id, self.name, self.sd_name, self.dev_name, self.status, self.health, self.model, self.serial, self.firmware))
                self.id = cur.lastrowid
                conn.commit()
        finally:
            cur.close()

        for attribute in self.get_smart_attributes():
            attribute.save(self.id)

        return self.id

class SmartAttribute(object):

    id = None
    int_name = None
    str_name = None
    value = None
    health = None
    message = None

    def __init__(self, string, conn):
        self.conn = conn
        self.string = string
        self.int_name = self.set_int_name(string)
        self.str_name = self.set_str_name(string)
        self.value = self.set_value(string)
        self.health = self.set_health(string)

    def set_int_name(self, string):
        if self.int_name:
            return self.int_name
        else:
            return string.split()[0]

    def set_str_name(self, string):
        if self.str_name:
            return self.str_name
        else:
            return string.split()[1]

    def set_value(self, string):
        if self.value:
            return self.value
        else:
            return string.split()[9]

    def get_previous(self, int_name):

        return int_name

    def set_health(self, string):
        if self.health:
            return 0
        else:
            int = self.set_int_name(string)
            str = self.set_str_name(string)
            val = self.set_value(string)
            if 196 <= int <= 199 and val > 0:
                prev_val = self.get_previous_value(int)
                if prev_val == val:
                    return 0
                else:
                    self.message = "Smart Attribute %s - %s : Value %s -> %s" % (str(int), str, str(prev_val), str(val))
                    if int == 199:
                        return 1
                    else:
                        return 3

    def save(self, parent_id):
        cur = self.conn.cursor()
        try:
            cur.execute('select rowid from smart_attributes WHERE sd_device_id = ? and int_name = ?', (parent_id, self.int_name))
            rowid = cur.fetchone()
            if rowid:
                self.id = rowid[0]
            else:
                cur.execute(
                    'insert into smart_attributes(sd_device_id, int_name, str_name, value) values(?,?,?,?)',
                    (parent_id, self.int_name, self.str_name, self.value))
                self.id = cur.lastrowid
                conn.commit()
        finally:
            cur.close()

        return self.id

def setupdb(filename, reset = False):

    conn = sqlite3.connect(filename)

    if reset:
        try:
            cur = conn.cursor()
            cur.execute('DROP TABLE IF EXISTS systems;')
            cur.execute('CREATE TABLE systems(hostname text, timeid varchar(20));')
            cur.execute('CREATE INDEX systems_hostname_idx ON systems(hostname);')
            cur.execute('CREATE INDEX systems_timeid_idx ON systems(timeid);')
            conn.commit()
            cur.close()
        except Exception, e:
            print `e`

        try:
            cur = conn.cursor()
            cur.execute('DROP TABLE IF EXISTS md_devices;')
            cur.execute('CREATE TABLE md_devices (system_id integer, name text, status text, level smallint, health text);')
            cur.execute('CREATE INDEX md_devices_system_id_idx ON md_devices(system_id);')
            cur.execute('CREATE INDEX md_devices_name_idx ON md_devices(name);')
            conn.commit()
            cur.close()
        except Exception, e:
            print `e`

        try:
            cur = conn.cursor()
            cur.execute('DROP TABLE IF EXISTS sd_devices;')
            cur.execute('CREATE TABLE sd_devices (md_device_id integer, name text, sd_name text, dev_name text, status text, health text, model text, serial text, firmware text);')
            cur.execute('CREATE INDEX sd_devices_md_device_id_idx ON sd_devices(md_device_id);')
            cur.execute('CREATE INDEX sd_devices_name_idx ON sd_devices(name);')
            conn.commit()
            cur.close()
        except Exception, e:
            print `e`

        try:
            cur = conn.cursor()
            cur.execute('DROP TABLE IF EXISTS smart_attributes;')
            cur.execute('CREATE TABLE smart_attributes (sd_device_id integer, int_name integer, str_name text, value text);')
            cur.execute('CREATE INDEX smart_attributes_sd_device_id_idx ON smart_attributes(sd_device_id);')
            cur.execute('CREATE INDEX smart_attributes_int_name_idx ON smart_attributes(int_name);')
            cur.execute('CREATE INDEX smart_attributes_str_name_idx ON smart_attributes(str_name);')
            conn.commit()
            cur.close()
        except Exception, e:
            print `e`

    return conn


if __name__ == '__main__':

    db_filename = WORKDIR + sys.argv[1] + '.db' if len(sys.argv) > 1 else 'RaidCheck-%s-%s.db' % (HOSTNAME, EXEC_TIME,)

    #if len(sys.argv) == 3 and sys.argv[2] == 'export':
    #    export = True
    #else:
    #    export = False


#    dev = Device('sde1', '200')
#    for attribute in dev.get_smart_attributes():
#        print '      %s: %s'%(attribute.str_name, attribute.value)

    reset = False
    if not os.path.isfile(db_filename):
        reset = True
        print 'Creating new SQLite file: ', db_filename
    else:
        print 'Using SQLite file: ', db_filename

    conn = setupdb(db_filename, reset)

    system = System(HOSTNAME, EXEC_TIME, conn)

    print system.hostname
    for md in system.md_devices:
        #print '########################################'
        print md.name
        for device in md.devices:
            print '  %s'%device.dev_name
            print '    %s'%device.model
            print '    %s'%device.serial
            print '    %s'%device.firmware
            for attribute in device.get_smart_attributes():
                print '      %s: %s'%(attribute.str_name, attribute.value)

    system.save()
