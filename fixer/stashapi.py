import re
import time
import logging
import datetime
import requests
import requests.packages.urllib3.util.retry as urllib_retry
import requests.adapters as requests_adapters
import rapidjson as json

POE_STASH_API_ENDPOINT = 'http://www.pathofexile.com/api/public-stash-tabs'

def requests_context():
    session = requests.Session()
    retry = urllib_retry.Retry(
        total=10,
        backoff_factor=1,
        status_forcelist=(500, 502, 503, 504))
    adapter = requests_adapters.HTTPAdapter(max_retries=retry)

    session.mount('http://', adapter)
    session.mount('https://', adapter)

    return session

class PoeApiData:

    fields = None
    required_fields = None

    def __init_subclass__(cls):
        def data_getter(name):
            return property(lambda  self: self._data.get(name, None))

        super().__init_subclass__()
        assert cls.fields, "Incorrectly initialized PoeApiData class"
        added = []
        for field in cls.fields:
            if field.startswith('_'):
                raise KeyError("Invalid field name: %s" % field)
            if not hasattr(cls, field):
                added += [field]
                setattr(cls, field, data_getter(field))

    def __init__(self, data, logger=logging):
        self._data = data
        self._logger = logger

    def _repr_fields(self):
        def format_fields():
            for field in sorted(self.fields):
                value = getattr(self, field)
                if value is None:
                    continue
                elif isinstance(value, str) and value.startswith('http'):
                    if len(value) > 10:
                        value = value[0:7] + '...'
                yield "%s=%r" % (field, value)
        return ", ".join(format_fields())

    def __repr__(self):
        if self.fields:
            return "<%s(%s)>" % (self.__class__.__name__, self._repr_fields())
        else:
            return "<%s()>" % self.__class__.__name__

    def validate(self):
        if self.required_fields:
            for field in self.required_fields:
                value = self._data.get(field, None)
                if value is None:
                    raise ValueError(
                        "%s: %s is a required field" % (
                            self.__class__.__name__, field))


class ApiItem(PoeApiData):

    name_cleaner_re = re.compile(r'^\<\<.*\>\>')
    fields = [
        "abyssJewel", "additionalProperties", "artFilename",
        "category", "corrupted", "cosmeticMods", "craftedMods",
        "descrText", "duplicated", "elder", "enchantMods",
        "explicitMods", "flavourText", "frameType", "h", "icon",
        "id", "identified", "ilvl", "implicitMods", "inventoryId",
        "isRelic", "league", "lockedToCharacter", "maxStackSize", "name",
        "nextLevelRequirements", "note", "properties", "prophecyDiffText",
        "prophecyText", "requirements", "secDescrText", "shaper",
        "socketedItems", "sockets", "stackSize", "support",
        "talismanTier", "typeLine", "utilityMods", "verified", "w", "x",
        "y"]

    required_fields = [
        "category", "id", "h", "w", "x", "y", "frameType", "icon",
        "identified", "ilvl", "league", "name", "typeLine", "verified"]

    def _clean_markup(self, value):
        return re.sub(self.name_cleaner_re, '', value)
    @property
    def typeLine(self):
        return self._clean_markup(self._data['typeLine'])

    @property
    def name(self):
        return self._clean_markup(self._data['name'])


class ApiStash(PoeApiData):

    fields = [
        'accountName', 'lastCharacterName', 'id', 'stash', 'stashType',
        'items', 'public']

    required_fields = ['id', 'stashType', 'public']

    @property
    def items(self):

        for item in self._data['items']:
            api_item = ApiItem(item)
            try:
                api_item.validate()
            except ValueError as e:
                self._logger.warning("Invalid item: %s", str(e))
                continue
            yield api_item

    @property
    def api_item_count(self):
        return len(self._data['items'])


class PoeApi:

    api_root = POE_STASH_API_ENDPOINT
    next_id = None
    rate = 1.1
    slow = False

    def __init__(
            self,
            next_id=None, rate=None, slow=None, api_root=None, logger=logging):
        self.logger = logger
        self.next_id = next_id
        if rate is not None:
            self.rate = datetime.timedelta(seconds=rate)
        if slow is not None:
            self.slow = slow
        if api_root is not None:
            self.api_root = api_root
        self.last_time = None
        self.rq_context = requests_context()

    def rate_wait(self):
        if self.last_time:
            now = datetime.datetime.now()
            delta = now - self.last_time
            if delta.total_seconds() < self.rate:
                remaining = self.rate - delta.total_seconds()
                time.sleep(remaining)
        self.set_last_time()

    def set_last_time(self):
        self.last_time = datetime.datetime.now()

    def get_next(self):
        self.rate_wait()
        data, self.next_id = self._get_data(next_id=self.next_id, slow=self.slow)
        return self.stash_generator(data)

    @staticmethod
    def stash_generator(data):
        for stash in data:
            api_stash = ApiStash(stash)
            try:
                api_stash.validate()
            except ValueError as e:
                self.logger.warning("Invalid stash: %s", str(e))
                continue
            yield api_stash

    def _get_data(self, next_id=None, slow=False):
        url = self.api_root
        if next_id:
            self.logger.info("Requesting next stash set: %s" % next_id)
            url += '?id=' + next_id
        else:
            self.logger.info("Requesting first stash set")
        req = self.rq_context.get(url)
        if slow:
            self.set_last_time()
        req.raise_for_status()
        self.logger.debug("Acquired stash data")
        data = json.loads(req.text)
        self.logger.debug("Loaded stash data from JSON")
        if 'next_change_id' not in data:
            raise KeyError('next_change_id required field not present in response')
        return (data['stashes'], data['next_change_id'])

if __name__ == '__main__':
    api = PoeApi()
    stashes = api.get_next()
    print("got first set of stashes")
    stashes = api.get_next()
    print("Next_id is %s" % api.next_id)
    done = False
    for input_stash in stashes:
        for stashitem in input_stash.items:
            print(
                "stash contains item: %s %s" % (stashitem.name, stashitem.typeLine))
            done = True
            break
        if done:
            break