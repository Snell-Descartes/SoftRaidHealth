
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


class System(object):

    id = None

    def __init__(self, hostname, timeid):
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
                md = MdDevice(md_string.group(0), self.timeid)
                md_devices.append(md)
        return md_devices

    def save(self, conn):
        cur = conn.cursor()
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
            md_dev.save(conn, self.id)

        return self.id

class MdDevice(object):

    id = None

    def __init__(self, string, timeid):
        self.timeid = timeid
        self.string = string
        self.name = self.set_name(string)
        self.health = self.set_health()
        self.status = self.set_status()
        self.level = self.set_level()
        self.devices = self.set_devices(string)

    def set_name(self, string):
        id_regex = re.compile(r"md[0-9]{0,3}", re.IGNORECASE)
        for id in id_regex.finditer(string):
            return id.group(0)

    def set_health(self):
        return None

    def set_status(self):
        return None

    def set_level(self):
        return None

    def set_devices(self, string):
        dev_regex = re.compile(r"sd[a-z]{1,2}[0-9]{1,2}\[[0-9{1-2}]\]", re.IGNORECASE)
        sd_dev = []
        for sd_string in dev_regex.finditer(string):
            sd_dev.append(Device(sd_string.group(0), self.timeid))
        return sd_dev

    def save(self, conn, parent_id):
        cur = conn.cursor()
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
            sd_dev.save(conn, self.id)

        return self.id

class Device(object):

    smart_output = None
    id = None
    smart_attributes = None

    def __init__(self, string, timeid):
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
        return None

    def set_health(self):
        return None

    def get_smart_output(self):
        if self.smart_output:
            return self.smart_output
        else:
            self.smart_output = subprocess.Popen(["smartctl", "-a", "/dev/%s" % self.sd_name], stdout=subprocess.PIPE).communicate()[0]
            return self.smart_output

    def get_smart_attributes(self):
        attributes_regex = re.compile(r"(RAW_VALUE\n[\]{1, 3}[0 - 9]{1, 3}.*.(SMART Error))", re.DOTALL)
        for attributes in attributes_regex.finditer(self.get_smart_output()):
        return attributes.group(0).split(':')[1].replace('RAW_VALUE\n','').replace('SMART Error','').strip()
        #return "1 Raw_Read_Error_Rate 0x002f 200 200 051 Pre-fail Always - 0\n3 Spin_Up_Time 0x0027 178 177 021 Pre-fail Always - 6091\n4 Start_Stop_Count 0x0032 100 100 000 Old_age Always - 56\n5 Reallocated_Sector_Ct 0x0033 200 200 140 Pre-fail Always - 0\n7 Seek_Error_Rate 0x002e 200 200 000 Old_age Always - 0\n9 Power_On_Hours 0x0032 092 092 000 Old_age Always - 5863\n10 Spin_Retry_Count 0x0032 100 253 000 Old_age Always - 0\n11 Calibration_Retry_Count 0x0032 100 253 000 Old_age Always - 0\n12 Power_Cycle_Count 0x0032 100 100 000 Old_age Always - 51\n192 Power-Off_Retract_Count 0x0032 200 200 000 Old_age Always - 50\n193 Load_Cycle_Count 0x0032 197 197 000 Old_age Always - 11001\n194 Temperature_Celsius 0x0022 114 106 000 Old_age Always - 36\n196 Reallocated_Event_Count 0x0032 200 200 000 Old_age Always - 0\n197 Current_Pending_Sector 0x0032 200 200 000 Old_age Always - 0\n198 Offline_Uncorrectable 0x0030 200 200 000 Old_age Offline - 0\n199 UDMA_CRC_Error_Count 0x0032 200 192 000 Old_age Always - 1278\n200 Multi_Zone_Error_Rate 0x0008 200 200 000 Old_age Offline - 0"

    def smart_attributes(self):
        line_regex = re.compile(r"[0-9].*", re.MULTILINE)
        attributes = []
        for line in line_regex.finditer(self.get_smart_attributes()):
            attributes.append(SmartAttribute(line.group(0)))
        return attributes

    # def smart_attributes(self):
    #     line_regex = re.compile(r"[0-9].*", re.MULTILINE)
    #     attributes = {}
    #     for lines in line_regex.finditer(self.get_smart_attributes()):
    #         data = lines.split(' ')
    #         attributes[data[0]] = [data[1], data[10]]
    #     return attributes

    # def yo
    #     smart_attributes = []
    #     for attribute in self.smart_attributes():
    #         smart_attributes

    def save(self, conn, parent_id):
        cur = conn.cursor()
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

        for attribute in self.smart_attributes():
            attribute.save(conn, self.id)

        return self.id

class SmartAttribute(object):

    id = None

    def __init__(self, string):
        self.string = string
        self.int_name = self.set_int_name(string)
        self.str_name = self.set_str_name(string)
        self.value = self.set_value(string)

    def set_int_name(self, string):
        return string.split(' ')[0]

    def set_str_name(self, string):
        return string.split(' ')[1]

    def set_value(self, string):
        return string.split(' ')[9]

    def save(self, conn, parent_id):
        cur = conn.cursor()
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
    system = System(HOSTNAME, EXEC_TIME)

    db_filename = WORKDIR + sys.argv[1] + '.db' if len(sys.argv) > 1 else 'RaidCheck-%s-%s.db' % (HOSTNAME, EXEC_TIME,)

    # if len(sys.argv) == 3 and sys.argv[2] == 'export':
    #     export = True
    # else:
    #     export = False

    reset = False
    if not os.path.isfile(db_filename):
        reset = True
        print 'Creating new SQLite file: ', db_filename
    else:
        print 'Using SQLite file: ', db_filename

    conn = setupdb(db_filename, reset)

    print system.hostname
    for md in system.md_devices:
        #print '########################################'
        print md.name
        for device in md.devices:
            print '  %s'%device.dev_name
            print '    %s'%device.model
            print '    %s'%device.serial
            print '    %s'%device.firmware

    system.save(conn)
