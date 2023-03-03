from db_func import *
import time
from mqtt_thread import paho

STABLE = 'ST,+0799.696969   g'
UNSTABLE = 'US,+0033.77777    g'


def get_th(hmi_id):
    print('[PROGRAM] Get Threshold')
    query = 'select hmi.*, sku.th_H, sku.th_L from hmi left join sku on hmi.sku_id = sku.id where hmi.id = %s' % hmi_id
    hmi = db_fetchone(query)
    return hmi
    # for row in hmi:
    # print(row['th_H'])


def auto_update():
    print('[PROGRAM] Auto update running')
    query = """select hmi.*,line.line_name,machine.machine_name,shift.shift_name,shift.shift_group,shift.shift_start,shift.shift_end,sku.sku_name,sku.target,sku.th_H,sku.th_L,pic.pic_name,pic.nik from hmi left join line on hmi.line_id=line.id 
    left join machine on hmi.machine_id=machine.id 
    left join shift on hmi.shift_id=shift.id 
    left join sku on hmi.sku_id=sku.id 
    left join pic on hmi.pic_id=pic.id """
    hmi = db_fetch(query)
    for row in hmi:
        # print(hmi)
        if row['auto'] and row['stable'] and row['weight'] >= row['hmi_th'] and not row['sending']:
            # print(row)
            # updating
            print('[MYSQL] INSERTING LOG')
            query = """insert into historical_log(line_name, machine_name, shift_name, shift_group, shift_start, shift_end, sku_name, hmi_name, weight, target, th_H, th_L, status, working_date, user, pic, nik) values('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, %s, %s, %s, '%s', SUBTIME(now(), "7:0:0"), '%s', '%s', '%s')""" % (
                row['line_name'], row['machine_name'], row['shift_name'], row['shift_group'], row['shift_start'], row['shift_end'], row['sku_name'], row['hmi_name'], row['weight'], row['target'], row['th_H'], row['th_L'], row['status'], row['user'], row['pic_name'], row['nik'])
            # print(query)
            db_query(query)
            query = "update hmi set sending=1 where id=%s" % row['id']
            db_query(query)


def parsing(raw_string, hmi_id):
    print('[PROGRAM] Parsing')
    data = raw_string.split(' ')[0]
    stable = 1 if data.split(',')[0] == 'ST' else 0
    weight = round(float(data.split(',')[1]), 3)
    hmi = get_th(hmi_id)
    th_H = hmi['th_H']
    th_L = hmi['th_L']
    status = 'UNDER'
    if weight >= th_L and weight <= th_H:
        status = 'PASS'
    elif weight < th_L:
        status = 'UNDER'
    elif weight > th_H:
        status = 'OVER'
    if stable:
        query = "update hmi set weight = %s, stable = %s, status='%s' where id =%s" % (
            weight, stable, status, hmi_id)
        db_query(query)
    else:
        query = "update hmi set weight = %s, stable = %s, status='%s', sending = 0 where id =%s" % (
            weight, stable, status, hmi_id)
        db_query(query)
    print('[MYSQL] Query : %s' % query)
    # print(query)


def auto_shift():
    print('[MYSQL] Update shift')
    query = """SELECT * FROM shift
    WHERE
        (shift_start < shift_end AND now() BETWEEN shift_start AND shift_end)
    OR
        (shift_end < shift_start AND now() < shift_start AND now() < shift_end)
    OR
        (shift_end < shift_start AND now() > shift_start)"""
    current_shift = db_fetchone(query)
    query = "update hmi set shift_id=%s" % current_shift['id']
    db_query(query)
    # print(query)


if __name__ == "__main__":
    try:
        db_connect()
        mqtt_thread = paho()
        mqtt_thread.start()
        while 1:
            if db_status():
                try:
                    # mqtt_thread = paho()
                    # get from mqttt here
                    # parsing(STABLE, 2)
                    # parsing(UNSTABLE, 2)
                    auto_update()
                    # auto_shift()
                    time.sleep(1/50)
                except KeyboardInterrupt:
                    print('[PROGRAM] Closed')
                    exit()
                except Exception as e:
                    print('[PROGRAM] Something is wrong')
                    print(e)
                    time.sleep(1/1000000)
            else:
                db_reconnect()
                time.sleep(1)

    except KeyboardInterrupt:
        print('[PROGRAM] Closed')
        exit()
    except Exception as e:
        print('[PROGRAM] Something is wrong')
        print(e)
        time.sleep(1/1000000)
    finally:
        mqtt_thread.close()
        db_close()
