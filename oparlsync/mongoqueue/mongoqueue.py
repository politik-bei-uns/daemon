#   Copyright 2012 Kapil Thangavelu
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.


import pymongo

from datetime import datetime, timedelta
import traceback


DEFAULT_INSERT = {
    "attempts": 0,
    "locked_by": None,
    "locked_at": None,
    "last_error": None
}


class MongoQueue(object):
    """A queue class
    """

    def __init__(self, collection, consumer_id, timeout=300, max_attempts=3):
        """
        """
        self.collection = collection
        self.consumer_id = consumer_id
        self.timeout = timeout
        self.max_attempts = max_attempts

    def close(self):
        """Close the in memory queue connection.
        """
        self.collection.connection.close()

    def clear(self):
        """Clear the queue.
        """
        return self.collection.drop()

    def clear_safe(self):
        return self.collection.remove({
            "locked_by": None,
            "locked_at": None
        })

    def size(self):
        """Total size of the queue
        """
        return self.collection.count()

    def repair(self):
        """Clear out stale locks.

        Increments per job attempt counter.
        """
        self.collection.find_and_modify(
            query={
                "locked_by": {"$ne": None},
                "locked_at": {
                    "$lt": datetime.now() - timedelta(self.timeout)
                }
            },
            update={
                "$set": {"locked_by": None, "locked_at": None},
                "$inc": {"attempts": 1}
            }
        )

    def get_running_externals(self):
        externals = []
        for job in self.collection.find():
            if 'external' in job and job['locked_by'] is not None and job['locked_at'] is not None:
                if job['external']:
                    externals.append(job['external'])
        return externals

    def drop_max_attempts(self):
        """
        """
        self.collection.find_and_modify(
            {"attempts": {"$gte": self.max_attempts}},
            remove=True)

    def put(self, payload, external=None, priority=0):
        """Place a job into the queue
        """
        job = dict(DEFAULT_INSERT)
        job['priority'] = priority
        job['payload'] = payload
        job['external'] = external
        return self.collection.insert(job)

    def next(self):
        print(self.get_running_externals())
        return self._wrap_one(self.collection.find_and_modify(
            query={
                "locked_by": None,
                "locked_at": None,
                "attempts": {
                    "$lt": self.max_attempts
                },
                "external": {
                    "$nin": self.get_running_externals()
                }

            },
            update={
                "$set": {
                    "locked_by": self.consumer_id,
                    "locked_at": datetime.now()
                }
            },
            sort=[('priority', pymongo.DESCENDING)],
            new=1,
            limit=1
        ))

    def _jobs(self):
        return self.collection.find(
            query={
                "locked_by": None,
                "locked_at": None,
                "attempts": {"$lt": self.max_attempts}
            },
            sort=[('priority', pymongo.DESCENDING)]
        )

    def _wrap_one(self, data):
        return data and Job(self, data) or None

    def stats(self):
        """Get statistics on the queue.

        """
        stats = {
            "available": 0,
            "locked": 0,
            "errors": 0,
            "total": 0
        }
        items = []
        for job in self.collection.find():
            item = job['payload']
            if job['locked_by'] != None:
                stats['locked'] += 1
                item['status'] = 'locked'
            elif job['attempts'] > self.max_attempts:
                stats['errors'] += 1
                item['status'] = 'error'
            else:
                stats['available'] += 1
                item['status'] = 'available'
            stats['total'] += 1
            items.append(item)
        return stats

    def details(self):
        """Get details on the queue.

        """
        items = []
        for job in self.collection.find():
            item = job['payload']
            if job['locked_by'] != None:
                item['status'] = 'locked'
            elif job['attempts'] > self.max_attempts:
                item['status'] = 'error'
            else:
                item['status'] = 'available'
            items.append(item)
        return items


class Job(object):

    def __init__(self, queue, data):
        """
        """
        self._queue = queue
        self._data = data

    @property
    def payload(self):
        return self._data['payload']

    @property
    def job_id(self):
        return self._data["_id"]

    @property
    def priority(self):
        return self._data["priority"]

    @property
    def attempts(self):
        return self._data["attempts"]

    @property
    def locked_by(self):
        return self._data["locked_by"]

    @property
    def locked_at(self):
        return self._data["locked_at"]

    @property
    def last_error(self):
        return self._data["last_error"]

    ## Job Control

    def complete(self):
        """Job has been completed.
        """
        return self._queue.collection.find_and_modify(
            {"_id": self.job_id, "locked_by": self._queue.consumer_id},
            remove=True)

    def error(self, message=None):
        """Note an error processing a job, and return it to the queue.
        """
        self._queue.collection.find_and_modify(
            {"_id": self.job_id, "locked_by": self._queue.consumer_id},
            update={"$set": {
                "locked_by": None, "locked_at": None, "last_error": message},
                "$inc": {"attempts": 1}})

    def progress(self, count=0):
        """Note progress on a long running task.
        """
        return self._queue.collection.find_and_modify(
            {
                "_id": self.job_id,
                "locked_by": self._queue.consumer_id
            },
            update={
                "$set": {"progress": count, "locked_at": datetime.now()}
            }
        )

    def release(self):
        """Put the job back into_queue.
        """
        return self._queue.collection.find_and_modify(
            {
                "_id": self.job_id,
                "locked_by": self._queue.consumer_id
            },
            update={
                "$set": {"locked_by": None, "locked_at": None},
                "$inc": {"attempts": 1}
            }
        )

    ## Context Manager support

    def __enter__(self):
        return self._data

    def __exit__(self, type, value, tb):
        if (type, value, tb) == (None, None, None):
            self.complete()
        else:
            error = traceback.format_exc()
            self.error(error)
