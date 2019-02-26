import time
import math
import numpy
import logging
import datetime

import sqlalchemy

import fixer
from .currency_abbreviations import \
    PRICE_RE, PRICE_WITH_SPACE_RE, \
    OFFICIAL_CURRENCIES, UNOFFICIAL_CURRENCIES


class CurrencyPostprocessor:

    db = None
    start_time = None
    logger = None
    limit = None
    actual_currencies = {}
    recent = None
    relevant = int(datetime.timedelta(days=15).total_seconds())
    weight_increment = int(datetime.timedelta(hours=12).total_seconds())

    def __init__(self, db, start_time,
            continuous=False,
            recent=600,
            limit=None,
            logger=logging):
        self.db = db
        self.start_time = start_time
        self.continuous = continuous
        self.limit = limit
        self.logger = logger
        if recent is None or isinstance(recent, int):
            self.recent = recent
        elif isinstance(recent, datetime.timedelta):
            self.recent = recent.total_seconds()
        else:
            try:
                self.recent = int(recent)
            except:
                self.log("Invalid 'recent' caching parameter: %r", recent)
                raise

    def get_actual_currencies(self):

        def get_full_names():
            query = self.db.session.query(fixer.CurrencySummary)
            query = query.add_columns(fixer.CurrencySummary.from_currency)
            query = query.distinct()

            for row in query.all():
                yield row.from_currency

        def dashed(name):
            return name.replace(' ', '-')

        def dashed_clean(name):
            return dashed(name).replace("'", "")

        full_names = list(get_full_names())
        low = lambda name: name.lower()
        mapping = dict((low(name), name) for name in full_names)
        mapping.update(
            dict((dashed(low(name)), name) for name in full_names))
        mapping.update(
            dict((dashed_clean(low(name)), name) for name in full_names))

        self.logger.debug("Mapping of currencies: %r", mapping)

        return mapping

    def parse_note(self, note, regex=None):
        if note is not None:
            match = (regex or PRICE_RE).search(note)
            if match:
                try:
                    (sale_type, amt, currency) = match.groups()
                    low_cur = currency.lower()
                    if '/' in amt:
                        num, den = amt.split('/', 1)
                        amt = float(num) / float(den)
                    else:
                        amt = float(amt)
                    if  low_cur in OFFICIAL_CURRENCIES:
                        return (amt, OFFICIAL_CURRENCIES[low_cur])
                    elif low_cur in UNOFFICIAL_CURRENCIES:
                        return (amt, UNOFFICIAL_CURRENCIES[low_cur])
                    elif low_cur in self.actual_currencies:
                        return (amt, self.actual_currencies[low_cur])
                    elif currency:
                        if regex is None:
                            return self.parse_note(
                                note, regex=PRICE_WITH_SPACE_RE)
                        self.logger.warning(
                            "Currency note: %r has unknown currency abbrev %s",
                            note, currency)
                except ValueError as e:
                    if 'float' in str(e):
                        self.logger.debug("Invalid price: %r" % note)
                    else:
                        raise
        return (None, None)

    def _currency_query(self, start, block_size, offset):

        Item = fixer.Item

        query = self.db.session.query(fixer.Item)
        query = query.join(
            fixer.Stash,
            fixer.Stash.id == fixer.Item.stash_id)
        query = query.add_columns(
            fixer.Item.id,
            fixer.Item.api_id,
            fixer.Item.typeLine,
            fixer.Item.note,
            fixer.Item.updated_at,
            fixer.Stash.stash,
            fixer.Item.name,
            fixer.Stash.public)
        query = query.filter(fixer.Stash.public == True)
        if start is not None:
            query = query.filter(fixer.Item.updated_at >= start)
        query = query.order_by(
            Item.updated_at, Item.created_at, Item.id).limit(block_size)
        if offset:
            query = query.offset(offset)

        return query

    def _update_currency_pricing(
            self, name, currency, league, price, sale_time, is_currency):
        if is_currency:
            self._update_currency_summary(
                name, currency, league, price, sale_time)

        return self.find_value_of(currency, league, price)

    def _get_mean_and_std(self, name, currency, league, sale_time):

        def calc_mean_std(values, weights):
            mean = numpy.average(values, weights=weights)
            variance = numpy.average((values-mean)**2, weights=weights)
            stddev = math.sqrt(variance)

            return (mean, stddev)

        now = int(time.time())
        query = self.db.session.query(fixer.Sale)
        query = query.join(
            fixer.Item, fixer.Sale.item_id == fixer.Item.id)
        query = query.filter(fixer.Sale.name == name)
        query = query.filter(fixer.Item.league == league)
        query = query.filter(fixer.Sale.sale_currency == currency)
        query = query.filter(
            fixer.Sale.item_updated_at > (now-self.relevant))
        query = query.add_columns(
            fixer.Sale.sale_amount,
            fixer.Sale.item_updated_at)

        values = numpy.array([(
            row.sale_amount,
            self.weight_increment/max(1,sale_time-row.item_updated_at))
            for row in query.all()])
        if len(values) == 0:
            return (None, None, None, None)
        prices = values[:,0]
        weights = values[:,1]
        mean, stddev = calc_mean_std(prices, weights)
        count = len(prices)
        total_weight = weights.sum()

        if count > 3 and stddev > mean/2:
            self.logger.debug(
                "%s->%s: Large stddev=%s vs mean=%s, recalibrating",
                name, currency, stddev, mean)
            prices_ok = numpy.absolute(prices-mean) <= stddev*2
            prices = numpy.extract(prices_ok, prices)
            weights = numpy.extract(prices_ok, weights)
            mean, stddev = calc_mean_std(prices, weights)
            count2 = len(prices)
            total_weight = weights.sum()
            self.logger.debug(
                "Recalibration ignored %s rows, final stddev=%s, mean=%s",
                count - count2, stddev, mean)
            count = count2

        return (float(mean), float(stddev), float(total_weight), count)

    def _update_currency_summary(
            self, name, currency, league, price, sale_time):
        query = self.db.session.query(fixer.CurrencySummary)
        query = query.filter(fixer.CurrencySummary.from_currency == name)
        query = query.filter(fixer.CurrencySummary.to_currency == currency)
        query = query.filter(fixer.CurrencySummary.league == league)
        existing = query.one_or_none()

        now = int(time.time())

        if (
                self.recent and
                existing and
                existing.count >= 10 and
                existing and existing.updated_at >= now-self.recent):
            self.logger.debug(
                "Skipping cached currency: %s->%s %s(%s)",
                name, currency, league, price)
            return

        weighted_mean, weighted_stddev, weight, count = \
            self._get_mean_and_std(name, currency, league, sale_time)

        self.logger.debug(
            "Weighted stddev of sale of %s in %s = %s",
            name, currency, weighted_stddev)
        if weighted_stddev is None:
            return None

        if existing:
            cmd = sqlalchemy.sql.expression.update(fixer.CurrencySummary)
            cmd = cmd.where(
                fixer.CurrencySummary.from_currency == name)
            cmd = cmd.where(
                fixer.CurrencySummary.to_currency == currency)
            cmd = cmd.where(
                fixer.CurrencySummary.league == league)
            add_values = {}
        else:
            cmd = sqlalchemy.sql.expression.insert(fixer.CurrencySummary)
            add_values = {
                'from_currency': name,
                'to_currency': currency,
                'league': league,
                'created_at': int(time.time())}
        cmd = cmd.values(
            count=count,
            mean=weighted_mean,
            weight=weight,
            standard_dev=weighted_stddev,
            updated_at=int(time.time()), **add_values)
        self.db.session.execute(cmd)

    def find_value_of(self, name, league, price):

        if name == 'Chaos Orb':
            return price

        from_currency_field = fixer.CurrencySummary.from_currency
        to_currency_field = fixer.CurrencySummary.to_currency
        league_field = fixer.CurrencySummary.league

        query = self.db.session.query(fixer.CurrencySummary)
        query = query.filter(from_currency_field == name)
        query = query.filter(league_field == league)
        query = query.order_by(fixer.CurrencySummary.weight.desc())
        high_score = None
        conversion = None
        for row in query.all():
            target = row.to_currency
            if target == 'Chaos Orb':
                if not high_score or row.weight >= high_score:
                    self.logger.debug(
                        "Conversion discovered %s -> Chaos = %s",
                        name, row.mean)
                    high_score = row.weight
                    conversion = row.mean
                break
            if high_score and row.weight <= high_score:
                continue

            query2 = self.db.session.query(fixer.CurrencySummary)
            query2 = query2.filter(from_currency_field == target)
            query2 = query2.filter(to_currency_field == 'Chaos Orb')
            query2 = query2.filter(league_field == league)
            row2 = query2.one_or_none()
            if row2:
                score = min(row.weight, row2.weight)
                if (not high_score) or score > high_score:
                    high_score = score
                    conversion = row.mean * row2.mean
                    self.logger.debug(
                        "Conversion discovered %s -> %s (%s) -> Chaos (%s) = %s",
                        name, target, row.mean, row2.mean, conversion)

        if high_score:
            return conversion * price
        else:
            query = self.db.session.query(fixer.CurrencySummary)
            query = query.filter(from_currency_field == 'Chaos Orb')
            query = query.filter(to_currency_field == name)
            query = query.filter(league_field == league)
            row = query.one_or_none()

            if row:
                inverse = 1.0/row.mean
                if row:
                    self.logger.debug(
                        "Falling back on inverse Chaos -> %s pricing: %s",
                        name, inverse)
                    return inverse * price

        return None

    def _process_sale(self, row):
        if not (
                (row.Item.note and row.Item.note.startswith('~')) or
                row.stash.startswith('~')):
            return None
        is_currency = 'currency' in row.Item.category
        if is_currency:
            name = row.Item.typeLine
        else:
            name = (row.Item.name + " " + row.Item.typeLine).strip()
        pricing = row.Item.note
        stash_pricing = row.stash
        stash_price, stash_currency = self.parse_note(stash_pricing)
        price, currency = self.parse_note(pricing)
        if price is None:
            price, currency = (stash_price, stash_currency)
        if price is None or price == 0:
            return None
        existing = self.db.session.query(fixer.Sale).filter(
            fixer.Sale.item_id == row.Item.id).one_or_none()

        if not existing:
            existing = fixer.Sale(
                item_id=row.Item.id,
                item_api_id=row.Item.api_id,
                name=name,
                is_currency=is_currency,
                sale_currency=currency,
                sale_amount=price,
                sale_amount_chaos=None,
                created_at=int(time.time()),
                item_updated_at=row.Item.updated_at,
                updated_at=int(time.time()))
        else:
            existing.sale_currency = currency
            existing.sale_amount = price
            existing.sale_amount_chaos = None
            existing.item_updated_at = row.Item.updated_at
            existing.updated_at = int(time.time())

        self.db.session.add(existing)

        league = row.Item.league

        amount_chaos = self._update_currency_pricing(
            name, currency, league, price, row.Item.updated_at, is_currency)

        if amount_chaos is not None:
            self.logger.debug(
                "Found chaos value of %s -> %s %s = %s",
                name, price, currency, amount_chaos)

            existing.sale_amount_chaos = amount_chaos
            self.db.session.merge(existing)

        return existing.id

    def get_last_processed_time(self):
        query = self.db.session.query(fixer.Sale)
        query = query.order_by(fixer.Sale.item_updated_at.desc()).limit(1)
        result = query.one_or_none()
        if result:
            reference_time = result.item_updated_at
            when = time.strftime(
                "%Y-%m-%d %H:%M:%S",
                time.localtime(reference_time))
            self.logger.debug(
                "Last processed sale for item: %s(%s)",
                result.item_id, when)
            return reference_time
        return None


    def do_currency_postprocessor(self):

        def create_table(table, name):
            try:
                table.__table__.create(bind=self.db.session.bind)
            except (sqlalchemy.exc.OperationalError,
                    sqlalchemy.exc.InternalError) as e:
                if 'already exists' not in str(e):
                    raise
                self.logger.debug("%s table already exists.", name)
            else:
                self.logger.info("%s table created.", name)

        create_table(fixer.Sale, "Sale")
        create_table(fixer.CurrencySummary, "Currency Summary")

        prev = None
        while True:
            self.actual_currencies = self.get_actual_currencies()
            start = self.start_time or self.get_last_processed_time()
            if start:
                when = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))
                self.logger.info("Starting from %s", when)
            else:
                self.logger.info("Starting from beginning of item data.")
            (rows_done, last_row) = self._currency_processor_single_pass(start)
            if not prev or last_row != prev:
                prev = last_row
                self.logger.info("Processed %s rows in a pass", rows_done)
            elif self.continuous:
                time.sleep(1)

            if not self.continuous:
                break

    def _currency_processor_single_pass(self, start):

        offset = 0
        count = 0
        all_processed = 0
        todo = True
        block_size = 1000
        last_row = None

        while todo:
            query = self._currency_query(start, block_size, offset)
            count = 0
            for row in query.all():
                if not (row.Item.note or row.stash):
                    continue
                max_id = row.Item.id
                count += 1
                self.logger.debug("Row in %s" % row.Item.id)
                if count % 1000 == 0:
                    self.logger.info(
                        "%s rows in... (%s)",
                        count + offset, row.Item.updated_at)

                row_id = self._process_sale(row)

                if row_id:
                    last_row = row_id

            todo = count == block_size
            offset += count
            self.db.session.commit()
            all_processed += count
            if self.limit and all_processed > self.limit:
                break

        return (all_processed, last_row)