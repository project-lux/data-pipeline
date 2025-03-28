import logging
logger = logging.getLogger("lux_pipeline")

class Acquirer(object):
    def __init__(self, config):
        self.config = config
        self.configs = config["all_configs"]
        self.datacache = config["datacache"]
        self.recordcache = config["recordcache"]
        self.mapper = config["mapper"]
        self.fetcher = config["fetcher"]
        self.debug = config.get("debug", 0)
        self.name = config["name"]
        # self.force_rebuild = config.get("force_rebuild", False)
        self.validate = config.get("validate_on_acquire", False)
        self.raise_on_error = config.get("raise_on_validation_error", False)
        self.validator = config["all_configs"].validator
        self.ignore_sources = config.get("ignore_sources_for_records", [])

    def __getitem__(self, identifier):
        # allow rec = acquirer['ident'] code pattern
        # allow acquirer[ident##quaType] to set rectype
        params = self.configs.split_qua(identifier)
        return self.acquire(*params)

    def returns_multiple(self, record=None):
        return self.mapper.returns_multiple(record)

    def do_fetch(self, identifier, store=True, refetch=False):
        rec = None
        if not refetch:
            rec = self.datacache[identifier]
            if rec is not None:
                return rec

        # Don't have it, and config says to ignore
        # XXX Shouldn't this just be "enabled" per source?
        if self.name in self.ignore_sources:
            if self.debug:
                logger.debug(f"Skipping {self.name}/{identifier} in acquire due to ignore_sources")
            return None

        if self.fetcher.enabled:
            rec = self.fetcher.fetch(identifier)
            if rec is None:
                if self.debug:
                    logger.debug(f"Got no record fetching {self.name}/{identifier}; skipping")
                return None
            if "identifier" in rec and rec["identifier"] != identifier:
                # Might have been redirected
                identifier = rec["identifier"]
            if store:
                self.datacache[identifier] = rec
        else:
            # don't have it, can't fetch it...
            if self.debug:
                logger.debug(f"Fetcher for {self.name}/{identifier} disabled; skipping")
            return None
        return rec

    def do_post_map(self, rec, rectype, store):
        identifier = rec["identifier"]
        rec3 = self.mapper.post_mapping(rec, rectype)
        if rec3:
            if self.validate:
                errs = self.validator.validate(rec3)
                if errs:
                    # err... now what?
                    for error in errs:
                        logger.warning(f"  /{'/'.join([str(x) for x in error.absolute_path])} --> {error.message} ")
                    if self.raise_on_error:
                        raise ValueError(errs)
                    else:
                        return None
            if store:
                if self.config["type"] == "external":
                    try:
                        qrecid = self.configs.make_qua(rec3["identifier"], rectype)
                    except:
                        logger.critical(f"{rec3['identifier']} in {self.config['name']} has no type?? {rec3['data']}")
                        raise
                    rec3["identifier"] = qrecid
                    self.recordcache[qrecid] = rec3
                else:
                    self.recordcache[identifier] = rec3
        else:
            if self.debug:
                logger.debug(f"Post Mapping killed {rectype} record {self.name}/{identifier}")
        return rec3

    def acquire_all(self, identifier, store=True):
        if not self.mapper.returns_multiple():
            return None
        data = self.acquire(identifier, None, True, False, False)
        recs = self.mapper.transform_all(data)
        result = []
        for rec in recs:
            result.append(self.do_post_map(rec, rec["data"]["type"], store))
        return result

    def acquire(self, identifier, rectype=None, dataonly=False, store=True, reference=False, refetch=False):
        # Given an identifier, ensure that datacache and recordcache are populated
        # Return resulting record

        # validate identifier
        if self.fetcher and not self.fetcher.validate_identifier(identifier):
            if self.debug:
                logger.debug(f"{identifier} is not a valid identifier for {self.name}")
            return None

        # Return already built record
        if not dataonly and not reference and not refetch:
            rec = self.recordcache[identifier]
            if rec is not None:
                return rec
            elif rectype is not None and self.config["type"] == "external":
                qrecid = self.configs.make_qua(identifier, rectype)
                rec = self.recordcache[qrecid]
                if rec is not None:
                    return rec

        rec = self.do_fetch(identifier, store, refetch)
        if dataonly or rec is None:
            return rec

        identifier = rec["identifier"]  # might have mutated during fetch
        if rectype is None and "type" in rec["data"]:
            if type(rec["data"]["type"]) == list:
                rec["data"]["type"].remove("Type")  # Prefer Language/Material to Type
                rec["data"]["type"] = rec["data"]["type"][0]  # and pick the first, right or wrong
            if type(rec["data"]["type"]) == dict:
                logger.debug(f'{rec["identifier"]} -> {rec["data"]["type"]}')
                return None

            if rec["data"]["type"] in self.configs.ok_record_types:
                rectype = rec["data"]["type"]

        try:
            rec2 = self.mapper.transform(rec, rectype, reference=reference)
        except Exception as e:
            logger.warning(f"Failed to map record {identifier} for {self.name}: {e}")
            return None

        if rec2 is None:
            if self.debug:
                logger.debug(f"Cannot map {self.name}/{identifier} into Linked Art (as {rectype})")
            return None
        rec2["identifier"] = identifier

        if reference:
            return rec2

        rec3 = self.do_post_map(rec2, rec2["data"]["type"], store=store)
        return rec3
