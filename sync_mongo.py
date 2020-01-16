import pymongo
import os
import logging
import json
from bson.timestamp import Timestamp
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class SyncMongo():
    _max_buffer_size = 512

    def __init__(self, uri, db, colname, clone_collection_name):
        self._buffer = []
        self._clone_collection_name = clone_collection_name
        self.start_at = None
        self.client = pymongo.mongo_client.MongoClient(uri)
        self.db = self.client[db]
        self.col = self.db[colname]
        self._colname = colname

    def sync(self):
        if self._exists_resume_token():
            self._resume_from_token()
        elif self._is_initial_sync_required():
            self._initial_sync()
        else:
            self._resume_using_oplog_lastentry()

    def _exists_resume_token(self):
        record = self.db["sync"].find_one({ "collection": self._colname })
        return record is not None and "resume_token" in record

    def _resume_from_token(self):
        lastChange = { "_data": self.db["sync"].find_one({ "collection": self._colname }, { "resume_token": 1 })["resume_token"] }

        self._read_from_changestream(resume_after=lastChange)

    def _is_initial_sync_required(self):
        clone_exists = self._clone_collection_name in self.db.list_collection_names()

        if clone_exists:
            if not self._exists_last_entry():
                logger.error("Last entry does not exists, performing an initial sync")
                return True
            else:
                return False
        else:
            return True
    
    def _resume_using_oplog_lastentry(self):
        lastentry = Timestamp(self.db["sync"].find_one({ "collection": self._colname }, { "oplog_last_entry": 1 })["oplog_last_entry"], 0)

        logger.info("reading from last entry of op log: %s " % lastentry)
        self._read_from_changestream(start_at_operation_time=lastentry)

    def _exists_last_entry(self):
        record = self.db["sync"].find_one({ "collection": self._colname })
        return record is not None and "oplog_last_entry" in record

    def _initial_sync(self):
        logger.info("starting initial sync")

        self.db[self._clone_collection_name].drop()
        self.col.aggregate([ { "$out": self._clone_collection_name } ])

        self._save_last_entry_from_op_log()

        logger.info("finishing initial sync")

    def _read_from_changestream(self, resume_after=None, start_at_operation_time=None):
        logger.info("reading from changestream")

        with self.col.watch([ { "$match": { "operationType": "insert" } } ], 
                       resume_after=resume_after, 
                       start_at_operation_time=start_at_operation_time, 
                       max_await_time_ms=10000) as cs:
            while cs.alive:
                doc =  cs.try_next()
                if doc is not None:
                    resume_after = doc["_id"]

                    self._push_to_buffer(doc["fullDocument"])

                    logger.debug(doc)
                else:
                    break

            self._flush_buffer()

        if resume_after is not None:
            self._save_changestream_last_entry(resume_after)

        logger.info("Completed read from log")

    def _flush_buffer(self):
        if len(self._buffer) > 0:
            logger.info("Flusing %d documents" % len(self._buffer))
            self.db[self._clone_collection_name].insert_many(self._buffer, ordered=False)
        self._buffer = []

    def _push_to_buffer(self, document):
        self._buffer.append(document)

        if len(self._buffer) >= self._max_buffer_size:
            self._flush_buffer()

    def _save_last_entry_from_op_log(self):
        logger.info("getting op log last entry")
        oplog_entry = self._get_oplog_lastentry()
        self.db["sync"].update_one({ "collection": self._colname }, { "$set": { "oplog_last_entry": oplog_entry["ts"].time } }, upsert=True)

    def _get_oplog_lastentry(self):
        return self.client["local"]["oplog.rs"].find().sort("ts", pymongo.DESCENDING).limit(1)[0]

    def _save_changestream_last_entry(self, data):
        logger.info("Writing last entry")
        self.db["sync"].update_one({ "collection": self._colname }, { "$set": { "resume_token": data["_data"] }, "$unset": { "oplog_last_entry": 1 } }, upsert=True)

