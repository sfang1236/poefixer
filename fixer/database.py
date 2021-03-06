import re
import time
import logging
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
import rapidjson as json

PoeDbBase = declarative_base()
PoeDbMetadata = PoeDbBase.metadata

class SemiJSON(sqlalchemy.types.TypeDecorator):
    impl = sqlalchemy.UnicodeText

    def load_dialect_impl(self, dialect):
        if dialect.name == 'sqlite':
            return dialect.type_descriptor(self.impl)
        return dialect.type_descriptor(sqlalchemy.JSON())

    def process_bind_param(self, value, dialect):
        if dialect.name == 'sqlite' and value is not None:
            value = json.dumps(value)
        return value

    def process_result_value(self, value, dialect):
        if dialect.name == 'sqlite' and value is not None:
            value = json.loads(value)
        return value

class Stash(PoeDbBase):

    __tablename__ = 'stash'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    api_id = sqlalchemy.Column(
        sqlalchemy.String(255), nullable=False, index=True, unique=True)
    accountName = sqlalchemy.Column(sqlalchemy.Unicode(255))
    lastCharacterName = sqlalchemy.Column(sqlalchemy.Unicode(255))
    stash = sqlalchemy.Column(sqlalchemy.Unicode(255))
    stashType = sqlalchemy.Column(sqlalchemy.Unicode(32), nullable=False)
    public = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, index=True)
    created_at = sqlalchemy.Column(
        sqlalchemy.Integer, nullable=False, index=True)
    updated_at = sqlalchemy.Column(
        sqlalchemy.Integer, nullable=False, index=True)

    def __repr__(self):
        return "<Stash(stash=%r, id=%s, api_id=%s)>" % (
            self.stash, self.id, self.api_id)


class Item(PoeDbBase):
    __tablename__ = 'item'


    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    api_id = sqlalchemy.Column(
        sqlalchemy.String(255), nullable=False, index=True, unique=True)
    stash_id = sqlalchemy.Column(
        sqlalchemy.Integer, sqlalchemy.ForeignKey("stash.id"),
        nullable=False)
    h = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    w = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    x = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    y = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    abyssJewel = sqlalchemy.Column(sqlalchemy.Boolean, default=False)
    artFilename = sqlalchemy.Column(sqlalchemy.String(255))
    category = sqlalchemy.Column(SemiJSON)
    corrupted = sqlalchemy.Column(sqlalchemy.Boolean, default=False)
    cosmeticMods = sqlalchemy.Column(SemiJSON)
    craftedMods = sqlalchemy.Column(SemiJSON)
    descrText = sqlalchemy.Column(sqlalchemy.Unicode(255))
    duplicated = sqlalchemy.Column(sqlalchemy.Boolean, default=False)
    elder = sqlalchemy.Column(sqlalchemy.Boolean, default=False)
    enchantMods = sqlalchemy.Column(SemiJSON)
    explicitMods = sqlalchemy.Column(SemiJSON)
    flavourText = sqlalchemy.Column(SemiJSON)
    frameType = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    icon = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    identified = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False)
    ilvl = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    implicitMods = sqlalchemy.Column(SemiJSON)
    inventoryId = sqlalchemy.Column(sqlalchemy.String(255))
    isRelic = sqlalchemy.Column(sqlalchemy.Boolean, default=False)
    league = sqlalchemy.Column(
        sqlalchemy.Unicode(64), nullable=False, index=True)
    lockedToCharacter = sqlalchemy.Column(sqlalchemy.Boolean, default=False)
    maxStackSize = sqlalchemy.Column(sqlalchemy.Integer)
    name = sqlalchemy.Column(
        sqlalchemy.Unicode(255), nullable=False, index=True)
    nextLevelRequirements = sqlalchemy.Column(SemiJSON)
    note = sqlalchemy.Column(sqlalchemy.Unicode(255))
    properties = sqlalchemy.Column(SemiJSON)
    prophecyDiffText = sqlalchemy.Column(sqlalchemy.Unicode(255))
    prophecyText = sqlalchemy.Column(sqlalchemy.Unicode(255))
    requirements = sqlalchemy.Column(SemiJSON)
    secDescrText = sqlalchemy.Column(sqlalchemy.Text)
    shaper = sqlalchemy.Column(sqlalchemy.Boolean, default=False)
    sockets = sqlalchemy.Column(SemiJSON)
    stackSize = sqlalchemy.Column(sqlalchemy.Integer)
    support = sqlalchemy.Column(sqlalchemy.Boolean, default=False)
    talismanTier = sqlalchemy.Column(sqlalchemy.Integer)
    typeLine = sqlalchemy.Column(
        sqlalchemy.String(255), nullable=False, index=True)
    utilityMods = sqlalchemy.Column(SemiJSON)
    verified = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False)
    active = sqlalchemy.Column(
        sqlalchemy.Boolean, nullable=False, default=True, index=True)
    created_at = sqlalchemy.Column(
        sqlalchemy.Integer, nullable=False, index=True)
    updated_at = sqlalchemy.Column(
        sqlalchemy.Integer, nullable=False, index=True)


    def __repr__(self):
        return "<Item(name=%r, id=%s, api_id=%s, typeLine=%r)>" % (
            self.name, self.id, self.api_id, self.typeLine)


class Sale(PoeDbBase):
    __tablename__ = 'sale'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    item_id = sqlalchemy.Column(
        sqlalchemy.Integer, sqlalchemy.ForeignKey("item.id"), nullable=False)
    item_api_id = sqlalchemy.Column(
        sqlalchemy.String(255), nullable=False, index=True, unique=True)
    name = sqlalchemy.Column(
        sqlalchemy.Unicode(255), nullable=False, index=True)
    is_currency = sqlalchemy.Column(
        sqlalchemy.Boolean, nullable=False, index=True)
    sale_currency = sqlalchemy.Column(
        sqlalchemy.Unicode(255), nullable=False, index=True)
    sale_amount = sqlalchemy.Column(sqlalchemy.Float)
    sale_amount_chaos = sqlalchemy.Column(sqlalchemy.Float)
    created_at = sqlalchemy.Column(
        sqlalchemy.Integer, nullable=False, index=True)
    item_updated_at = sqlalchemy.Column(
        sqlalchemy.Integer, nullable=False, index=True)
    updated_at = sqlalchemy.Column(
        sqlalchemy.Integer, nullable=False, index=True)

    def __repr__(self):
        return "<Sale(id=%s, item_id=%s, item_api_id=%s)>" % (
            self.id, self.item_id, self.item_api_id)

    def __str__(self):
        return (
            "Sale(%s) ItemId=%s ItemApiId=%s value=%s "
            "Chaos=%s Time=%s") % (
            self.id, self.item_id, self.item_api_id,
            self.sale_amount + " " + self.sale_currency,
            self.sale_amount_chaos)


class CurrencySummary(PoeDbBase):
    __tablename__ = 'currency_summary'


    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    from_currency = sqlalchemy.Column(
        sqlalchemy.Unicode(255), nullable=False)
    to_currency = sqlalchemy.Column(
        sqlalchemy.Unicode(255), nullable=False, index=True)
    league = sqlalchemy.Column(sqlalchemy.Unicode(64), nullable=False)
    count = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    weight = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    mean = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    standard_dev = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    created_at = sqlalchemy.Column(
        sqlalchemy.Integer, nullable=False, index=True)
    updated_at = sqlalchemy.Column(
        sqlalchemy.Integer, nullable=False, index=True)

    __table_args__ = (
        sqlalchemy.UniqueConstraint('from_currency', 'to_currency', 'league'),)


class PoeDb:
    db_connect = 'sqlite:///poetest.db'
    _safe_uri_re = re.compile(r'(?<=\:)([^:]*?)(?=\@)')
    _session = None
    _engine = None
    _session_maker = None

    stash_simple_fields = [
        "accountName", "lastCharacterName", "stash", "stashType",
        "public"]
    item_simple_fields = [
        "h", "w", "x", "y", "abyssJewel", "artFilename",
        "category", "corrupted", "cosmeticMods", "craftedMods",
        "descrText", "duplicated", "elder", "enchantMods",
        "explicitMods", "flavourText", "frameType", "icon",
        "identified", "ilvl", "implicitMods", "inventoryId",
        "isRelic", "league", "lockedToCharacter", "maxStackSize",
        "name", "nextLevelRequirements", "note", "properties",
        "prophecyDiffText", "prophecyText", "requirements",
        "secDescrText", "shaper", "sockets",
        "stackSize", "support", "talismanTier", "typeLine",
        "utilityMods", "verified"]

    def insert_api_stash(self, stash, with_items=False, keep_items=False):
        dbstash = self._insert_or_update_row(
            Stash, stash, self.stash_simple_fields)

        if with_items:
            self.session.flush()
            self.session.refresh(dbstash)
            self.logger.debug(
                "Injecting %s items for stash: %s",
                stash.api_item_count, stash.id)
            for item in stash.items:
                self._insert_or_update_row(
                    Item, item, self.item_simple_fields, stash=dbstash)

    def _invalidate_stash_items(self, dbstash):
        update = sqlalchemy.sql.expression.update(Item)
        update = update.where(Item.stash_id == dbstash.id)
        update = update.values(active=False)
        self.session.execute(update)

    def _insert_or_update_row(self, table, thing, simple_fields, stash=None):
        now = int(time.time())
        query = self.session.query(table)
        if thing.id:
            existing = query.filter(table.api_id == thing.id).one_or_none()
        else:
            existing = None
        if existing:
            row = existing
        else:
            row = table()
            row.created_at = now

        row.api_id = thing.id
        row.updated_at = now
        if stash:
            row.stash_id = stash.id
        if table == Item:
            row.active = True

        for field in simple_fields:
            setattr(row, field, getattr(thing, field, None))

        self.session.add(row)
        return row

    @property
    def session(self):
        if not self._session:
            self._session = self._session_maker()
        return self._session

    def create_database(self):
        PoeDbBase.metadata.create_all(self._engine)

    def _safe_uri(self, uri):
        return self._safe_uri_re.sub('******', uri)

    def __init__(self, db_connect=None, echo=False, logger=logging):
        self.logger=logger

        if db_connect is not None:
            self.logger.debug("Connect URI: %s", self._safe_uri(db_connect))
            self.db_connect = db_connect

        self._engine = sqlalchemy.create_engine(self.db_connect, echo=echo)
        self._session_maker = sqlalchemy.orm.sessionmaker(bind=self._engine)