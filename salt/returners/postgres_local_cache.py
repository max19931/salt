# -*- coding: utf-8 -*-
'''
Return data to a postgresql server

:maintainer:    None
:maturity:      New
:depends:       psycopg2
:platform:      all

To enable this returner the minion will need the psycopg2 installed and
the following values configured in the minion or master config::

    returner.postgres.host: 'salt'
    returner.postgres.user: 'salt'
    returner.postgres.passwd: 'salt'
    returner.postgres.db: 'salt'
    returner.postgres.port: 5432

Running the following commands as the postgres user should create the database
correctly::

    psql << EOF
    CREATE ROLE salt WITH PASSWORD 'salt';
    CREATE DATABASE salt WITH OWNER salt;
    EOF

    psql -h localhost -U salt << EOF
    --
    -- Table structure for table 'jids'
    --

    DROP TABLE IF EXISTS jids;
    CREATE TABLE jids (
      jid   varchar(20) PRIMARY KEY,
      load  text NOT NULL
    );

    --
    -- Table structure for table 'salt_returns'
    --

    DROP TABLE IF EXISTS salt_returns;
    CREATE TABLE salt_returns (
      added     TIMESTAMP WITH TIME ZONE DEFAULT now(),
      fun       text NOT NULL,
      jid       varchar(20) NOT NULL,
      return    text NOT NULL,
      id        text NOT NULL,
      success   boolean
    );
    CREATE INDEX ON salt_returns (added);
    CREATE INDEX ON salt_returns (id);
    CREATE INDEX ON salt_returns (jid);
    CREATE INDEX ON salt_returns (fun);
    EOF

Required python modules: psycopg2

  To use the postgres returner, append '--return postgres' to the salt command. ex:

    salt '*' test.ping --return postgres
'''
# Let's not allow PyLint complain about string substitution
# pylint: disable=W1321,E1321

# Import python libs
import json
import logging
import re

# Import salt libs
import salt.utils

log = logging.getLogger(__name__)

# Import third party libs
try:
    import psycopg2
    #import psycopg2.extras
    HAS_POSTGRES = True
except ImportError:
    HAS_POSTGRES = False


def __virtual__():
    if not HAS_POSTGRES:
        return False
    return 'postgres'


def _get_conn():
    '''
    Return a postgres connection.
    '''
    conn = psycopg2.connect(
            host=__opts__['master_job_cache.postgres.host'],
            user=__opts__['master_job_cache.postgres.user'],
            password=__opts__['master_job_cache.postgres.passwd'],
            database=__opts__['master_job_cache.postgres.db'],
            port=__opts__['master_job_cache.postgres.port'])
    return conn


def _close_conn(conn):
    conn.commit()
    conn.close()

def _format_job_instance(job):
    return {'Function': job.get('fun', 'unknown-function'),
            'Arguments': list(job.get('arg', [])),
            # unlikely but safeguard from invalid returns
            'Target': job.get('tgt', 'unknown-target'),
            'Target-type': job.get('tgt_type', []),
            'User': job.get('user', 'root')}


def _format_jid_instance(jid, job):
    ret = _format_job_instance(job)
    ret.update({'StartTime': salt.utils.jid_to_time(jid)})
    return ret

def returner(ret):
    '''
    Return data to a postgres server
    '''
    conn = _get_conn()
    cur = conn.cursor()
    sql = '''INSERT INTO salt_returns
            (fun, jid, return, id, success)
            VALUES (%s, %s, %s, %s, %s)'''
    cur.execute(
        sql, (
            ret['fun'],
            ret['jid'],
            json.dumps(ret['return']),
            ret['id'],
            ret['success']
        )
    )
    _close_conn(conn)


def save_load(jid, load):
    '''
    Save the load to the specified jid id
    '''
    jid = _escape_jid(jid)
    conn = _get_conn()
    cur = conn.cursor()
    sql = '''INSERT INTO jids (jid, load) VALUES (%s, %s)'''

    cur.execute(sql, (jid, json.dumps(load)))
    _close_conn(conn)

def _escape_jid(jid):
    jid = "%s" % jid
    jid = re.sub(r"'*", "", jid)
    return jid

def get_load(jid):
    '''
    Return the load data that marks a specified jid
    '''
    jid = _escape_jid(jid)
    conn = _get_conn()
    cur = conn.cursor()
    sql = '''SELECT load FROM jids WHERE jid = %s'''
    cur.execute(sql, (jid,))
    data = cur.fetchone()
    if data:
        return json.loads(data[0])
    _close_conn(conn)
    return {}


def get_jid(jid):
    '''
    Return the information returned when the specified job id was executed
    '''
    jid = _escape_jid(jid)
    conn = _get_conn()
    cur = conn.cursor()
    sql = '''SELECT id, return FROM salt_returns WHERE jid = %s'''

    cur.execute(sql, (jid,))
    data = cur.fetchall()
    ret = {}
    if data:
        for minion, full_ret in data:
            ret[minion] = {}
            ret[minion]['return'] = json.loads(full_ret)
    _close_conn(conn)
    return ret

def _gen_jid(cur):
    jid = salt.utils.gen_jid()
    sql = '''SELECT jid FROM jids WHERE jid = %s'''
    cur.execute(sql, (jid,))
    data = cur.fetchall()
    if not data:
        return jid
    return None

def prep_jid(nocache=False):
    '''
    Return a job id and prepare the job id directory
    This is the function responsible for making sure jids don't collide (unless its passed a jid)
    So do what you have to do to make sure that stays the case
    '''
    conn = _get_conn()
    cur = conn.cursor()
    jid = _gen_jid(cur)
    while not jid:
      log.info("jid clash, generating a new one")
      jid = _gen_jid(cur)

    cur.close()
    conn.close()
    return jid

def get_jids():
    '''
    Return a list of all job ids
    For master job cache this also formats the output and returns a string
    '''
    conn = _get_conn()
    cur = conn.cursor()
    sql = '''SELECT jid, load FROM jids'''

    cur.execute(sql)
    ret = {}
    data = cur.fetchone()
    while data:
        ret[data[0]] = _format_jid_instance(data[0], json.loads(data[1]))
        data = cur.fetchone()
    cur.close()
    conn.close()
    return ret

def clean_old_jobs():
    '''
    Clean out the old jobs from the job cache
    '''
    return
