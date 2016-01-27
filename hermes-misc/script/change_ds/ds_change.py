#!/usr/bin/env python
from httplib import HTTPConnection
import json
import traceback

import MySQLdb
from sql_generator import sql_generator
import xml.etree.ElementTree as ET

META_HOST = 'meta.hermes.fx.ctripcorp.com'
CAT_HOST = 'cat.ctripcorp.com'

META_PATH = 'XXXXX'

NEED_DUMMY = True

def get_meta():
    conn = HTTPConnection(META_HOST)
    conn.request('get', META_PATH)
    response = conn.getresponse()
    if response.status == 200:
        return json.loads(response.read(), 'utf-8')
    else:
        return None

def digit(ch, radix):
    try:
        return int(ch, radix)
    except:
        return -1

def decode_ds_pswd(source):
    if source.startswith('~{') and source.endswith('}'):
        source = source[2:len(source) - 1]
        
        a = bytearray()
        p = digit(source[0], 16) & 0x07
        q = digit(source[1], 16)
        k = digit(source[2], 16)
        for idx in range(3, len(source), 2):
            high = digit(source[idx], 16) & 0xFF
            low = digit(source[idx + 1], 16) & 0xFF
            a.append(high << 4 | low)
    
        for idx in range(0, len(a)):
            a[idx] = a[idx] ^ k
        
        length = len(a) * 8
        for idx in range(0, length, p):
            j = idx + q
            if j < length:
                b1 = a[idx / 8]
                b2 = a[j / 8]
                f1 = b1 & (1 << (idx % 8))
                f2 = b2 & (1 << (j % 8))
                if ((f1 != 0) != (f2 != 0)):
                    a[idx / 8] = a[idx / 8] ^ (1 << (idx % 8))
                    a[j / 8] = a[j / 8] ^ (1 << (j % 8))
        source = a[:len(a) - 13].decode('utf8')
    return source

def ensure_topic(meta):
    topic_ok = False
    while not topic_ok:
        topic = str(raw_input('1. Please enter topic name: '))
        if topic != None and len(topic.strip()) > 0:
            if not meta['topics'].__contains__(topic):
                print "\tTopic: {0} is not found.".format(topic)
            elif meta['topics'][topic]['storageType'] == 'kafka':
                print '\tTopic {0}\'s storage is kafka'.format(topic)
            else:
                topic_ok = True
                return meta['topics'][topic]

def ensure_partitions(topic):
    partitions = topic['partitions']
    print '\n\tTopic\t\t{0}'.format(topic['id'])
    print '\n\tPartitions\tID\tRead\tWrite'
    print '\t\t\t-------------------'
    for partition in partitions:
        print '\t\t\t{0}\t{1}\t{2}'.format(partition['id'], partition['readDatasource'], partition['writeDatasource'])
    
    contains = []
    input_str = str(raw_input('\n2. Please enter partitions (<ENTER> means all, or use "," for concat):'))
    if len(input_str.strip()) == 0:
        contains = partitions
    else:
        pids = set([ int(p.strip()) for p in input_str.split(',')])
        for partition in partitions:
            if pids.__contains__(partition['id']):
                contains.append(partition)
    print "\tChoosed partitions: " + str([partition['id'] for partition in contains])
    return contains
    
def ensure_consumers(topic):
    print '\n\tConsumers\tID\tName'
    print '\t\t\t------------------'
    for consumer in topic['consumerGroups']:
        print '\t\t\t{0}\t{1}'.format(consumer['id'], consumer['name'])
    print "\n3. Will create resend-table and offset-resend-table for above consumers."
    return topic['consumerGroups']


def get_datasources(meta):
    dses = {}
    for ds in meta['storages']['mysql']['datasources']:
        dses[ds['id']] = ds
    return dses

def ensure_datasource(dses):
    print '\n\tDatasources\tID\tJdbc'
    print '\t\t\t------------------'
    for (id, ds) in dses.items():  # @ReservedAssignment
        print '\t\t\t{0}\t{1}'.format(ds['id'], ds['properties']['url']['value'])
    
    print '\n'
    input_str = None;
    while not dses.__contains__(input_str):
        input_str = str(raw_input('4. Please enter target DS ID: '))
    print '\tChoosed DS: ' + input_str
    return dses[input_str]

def ensure_buffer(topic, dom):
    buf = -1
    qps = float(get_produce_qps_from_dom(dom, topic['name']))
    while buf < 5000:
        input_str = raw_input('5. Please enter message table BUFFER size [QPS: {0}]: '.format('{0} * 2h = {1}'.format(qps, qps * 60 * 60 * 2) if qps > 0 else 'UNKNOWN'))
        if len(input_str) > 0:
            buf = int(input_str)
            if buf < 5000:
                print 'Buffer should be more than 5000'
    return buf

def parse_url(jdbc_url):
    parts = jdbc_url.split('/')
    (host, port) = parts[2].split(':')
    db = "" if  len(parts) < 4 else parts[3]
    return (host, int(port), db)

def get_db_from_properties(props):
    (_host, _port, _db) = parse_url(props['url']['value'])
    _user = props['user']['value']
    _pwd = decode_ds_pswd(props['password']['value'])
    return MySQLdb.connect(host=_host, port=_port, db=_db, user=_user, passwd=_pwd)

def print_sql(name, content):
    print '\t##### {0} SQL:\t{1}'.format(name, content)
    

def create_dead_letter_table(tdb, tid, pid, buf, psize):
    tcursor = tdb.cursor()
    try:
        print '****** Initialing dead letter table: [topic: {0}, partition: {1}] '.format(tid, pid)
        create_sql = sql_generator.create_dead_letter_table(tid, pid, psize);
        print_sql('Create Dead Letter Table', create_sql)
        tcursor.execute(create_sql)
    except:
        traceback.print_exc()
    finally:
        tdb.commit()
        tcursor.close()

def create_offset_message_table(db, tdb, tid, pid):
    cursor = db.cursor()
    tcursor = tdb.cursor()
    try:
        print '****** Initialing offset message table: [topic: {0}, partition: {1}] '.format(tid, pid)
        create_sql = sql_generator.create_offset_message(tid, pid)
        print_sql('Create Offset Message Table', create_sql)
        tcursor.execute(create_sql)
        select_sql = sql_generator.select_offset_message(tid, pid)
        print_sql('Query Offset Message', select_sql)
        cursor.execute(select_sql)
        rows = cursor.fetchall()
        for row in rows:
            insert_sql = sql_generator.insert_offset_message(tid, pid, row)
            print_sql('Insert Offset Message', insert_sql)
            tcursor.execute(insert_sql)
    except:
        traceback.print_exc()
    finally:
        tdb.commit()
        cursor.close()
        tcursor.close()

def create_offset_resend_table(db, tdb, tid, pid):
    cursor = db.cursor()
    tcursor = tdb.cursor()
    try:
        print '****** Initialing offset resend table: [topic: {0}, partition: {1}] '.format(tid, pid)
        create_sql = sql_generator.create_offset_resend(tid, pid)
        print_sql('Create Offset Resend Table', create_sql)
        tcursor.execute(create_sql)
        select_sql = sql_generator.select_offset_resend(tid, pid)
        print_sql('Query Offset Resend', select_sql)
        cursor.execute(select_sql)
        rows = cursor.fetchall()
        for row in rows:
            insert_sql = sql_generator.insert_offset_resend(tid, pid, row)
            print_sql('Insert Offset Resend', insert_sql)
            tcursor.execute(insert_sql)
    except:
        traceback.print_exc()
    finally:
        tdb.commit()
        cursor.close()
        tcursor.close()

def create_message_table(db, tdb, tid, pid, pr, buf, psize, insert_dummy):
    cursor = db.cursor()
    tcursor = tdb.cursor()
    try:
        print '****** Initialing message table: [topic: {0}, partition: {1}, priority: {2}] '.format(tid, pid, pr)
        max_id_sql = sql_generator.select_message_max_id(tid, pid, pr)
        print_sql('Max Message ID', max_id_sql)
        cursor.execute(max_id_sql)
        max_id = cursor.fetchone()[0];
        max_id = 1 if max_id == None else max_id;
        create_sql = sql_generator.create_message_table(tid, pid, pr, max_id, buf, psize)
        print_sql('Create Message Table', create_sql)
        tcursor.execute(create_sql)
        if insert_dummy:
            dummy_sql = sql_generator.insert_dummy_into_message(tid, pid, pr)
            print_sql('Insert Dummy Row', dummy_sql)
            tcursor.execute(dummy_sql)
    except:
        traceback.print_exc()
    finally:
        tdb.commit()
        cursor.close()
        tcursor.close()
        
def create_resend_table(db, tdb, tid, pid, gid, buf, psize, insert_dummy):
    cursor = db.cursor()
    tcursor = tdb.cursor()
    try:
        print '****** Initialing resend table: [topic: {0}, partition: {1}, group: {2}]'.format(tid, pid, gid)
        max_id_sql = sql_generator.select_resend_max_id(tid, pid, gid)
        print_sql('Max Resend ID', max_id_sql)
        cursor.execute(max_id_sql)
        max_id = cursor.fetchone()[0];
        max_id = 1 if max_id == None else max_id;
        create_sql = sql_generator.create_resend_table(tid, pid, gid, max_id, buf, psize)
        print_sql('Create Resend Table', create_sql)
        tcursor.execute(create_sql)
        if insert_dummy:
            dummy_sql = sql_generator.insert_dummy_into_resend(tid, pid, gid)
            print_sql('Insert Dummy Row', dummy_sql)
            tcursor.execute(dummy_sql)
    except:
        traceback.print_exc()
    finally:
        tdb.commit()
        cursor.close()
        tcursor.close()

def delete_message_dummy(tdb, tid, pid, pr):
    tcursor = tdb.cursor()
    try:
        print '****** Deleting message table dummy row: [topic: {0}, partition: {1}, priority: {2}] '.format(tid, pid, pr)
        min_id_sql = sql_generator.select_message_min_id(tid, pid, pr)
        print_sql('Min Message ID', min_id_sql)
        tcursor.execute(min_id_sql)
        row = tcursor.fetchone()
        min_id = 1 if row == None else row[0]

        dummy_sql = sql_generator.delete_dummy_message(tid, pid, pr, min_id)
        print_sql('Delete Dummy Row', dummy_sql)
        tcursor.execute(dummy_sql)
    except:
        traceback.print_exc()
    finally:
        tdb.commit()
        tcursor.close()

def delete_resend_dummy(tdb, tid, pid, gid):
    tcursor = tdb.cursor()
    try:
        print '****** Deleting resend table dummy row: [topic: {0}, partition: {1}, consumer: {2}] '.format(tid, pid, gid)
        min_id_sql = sql_generator.select_resend_min_id(tid, pid, gid)
        print_sql('Min Message ID', min_id_sql)
        tcursor.execute(min_id_sql)
        row = tcursor.fetchone()
        min_id = 1 if row == None else row[0]
        
        dummy_sql = sql_generator.delete_dummy_resend(tid, pid, gid, min_id)
        print_sql('Delete Dummy Row', dummy_sql)
        tcursor.execute(dummy_sql)
    except:
        traceback.print_exc()
    finally:
        tdb.commit()
        tcursor.close()
        
def delete_dummy_rows(tdb, topic, partition, consumers):
    tid = topic['id']
    pid = partition['id']
    print '\n++++++++++++++  Starting delete dummy rows in target datasource !!  ++++++++++++++++++++++'
    delete_message_dummy(tdb, tid, pid, 0)
    delete_message_dummy(tdb, tid, pid, 1)
    for consumer in consumers:
        delete_resend_dummy(tdb, tid, pid, consumer['id'])

def init_target_db(db, tdb, topic, partition, consumers, buf, msg_psize, insert_dummy):
    tid = topic['id']
    pid = partition['id']
    print '\n++++++++++++++  Starting init tables in target datasource !!  ++++++++++++++++++++++'
    create_message_table(db, tdb, tid, pid, 0, buf, msg_psize, insert_dummy)
    create_message_table(db, tdb, tid, pid, 1, buf, msg_psize, insert_dummy)
    create_dead_letter_table(tdb, tid, pid, buf, msg_psize / 10)
    create_offset_message_table(db, tdb, tid, pid)
    create_offset_resend_table(db, tdb, tid, pid)
    for consumer in consumers:
        create_resend_table(db, tdb, tid, pid, consumer['id'], buf, msg_psize / 5, insert_dummy)

def get_test_db():
    return MySQLdb.connect(host="localhost", port=3306, db='test', user='root', passwd='')

def get_all_produce_dom_from_cat():
    print '\n\t***** Getting produce qps from cat ...'
    conn = HTTPConnection(CAT_HOST)
    conn.request('get', '/cat/r/t?op=graphs&domain=All&ip=All&type=Message.Produce.Tried&forceDownload=xml')
    response = conn.getresponse()
    return ET.fromstring(response.read()) if response.status == 200 else None

def get_produce_qps_from_dom(root, topic):
    if root == None:
        return -1
    
    node = root.find('report').find('machine[@ip="All"]//type//name[@id="{0}"]'.format(topic))
    return -1 if node == None else node.attrib['tps']

if __name__ == '__main__':
    meta = get_meta()
    topic = ensure_topic(meta)
    partitions = ensure_partitions(topic)
    consumers = ensure_consumers(topic)
    dses = get_datasources(meta)
#     tdb = get_db_from_properties(ensure_datasource(dses)['properties'])
    tdb = get_test_db()
    buf = ensure_buffer(topic, get_all_produce_dom_from_cat())
    
    tid = topic['id']
    for partition in partitions:
        db = get_db_from_properties(dses[partition['readDatasource']]['properties'])
        try:
            init_target_db(db, tdb, topic, partition, consumers, buf, 5000000, NEED_DUMMY)
        except:
            traceback.print_exc()
        finally:
            if not db == None:
                db.close()
        raw_input('\nPress any key to continue next partition ...')
    
    if NEED_DUMMY:
        input_str = 'init'
        while not input_str.strip() == 'yes' and not input_str.strip() == 'no':  
            input_str = raw_input('\nDo you want to delete dummy rows? (yes/no) ')
        if input_str.strip() == 'yes':
            for partition in partitions:
                delete_dummy_rows(tdb, topic, partition, consumers)
                raw_input('\nPress any key to continue next partition ...')
    tdb.close()