
class Acquirer(object):

    def __init__(self, config):
        self.config = config
        self.configs = config['all_configs']
        self.datacache = config['datacache']
        self.recordcache = config['recordcache']
        self.mapper = config['mapper']
        self.fetcher = config['fetcher']
        self.debug = config.get('debug', 0)
        self.name = config['name']
        self.force_rebuild = config.get('force_rebuild', False)
        self.validate = config.get('validate_on_acquire', False)
        self.raise_on_error = config.get('raise_on_validation_error', False)
        self.validator = config['all_configs'].validator
        self.ignore_sources = config.get('ignore_sources_for_records', [])

    def __getitem__(self, identifier):
        # allow rec = acquirer['ident'] code pattern
        return self.acquire(identifier)

    def acquire(self, identifier, rectype=None, dataonly=False, store=True, reference=False):
        # Given an identifier, ensure that datacache and recordcache are populated
        # Return resulting record

        # validate identifier
        if not self.fetcher.validate_identifier(identifier):
            if self.debug: print(f"{identifier} is not a valid identifier for {self.name}")
            return None

        # Return already built record
        if not dataonly and not reference:
            if identifier in self.recordcache:
                return self.recordcache[identifier]
            elif rectype is not None and self.config['type'] == 'external':
                qrecid = self.configs.make_qua(identifier, rectype)
                if qrecid in self.recordcache:
                    return self.recordcache[qrecid]

        if identifier in self.datacache and not self.force_rebuild:
            # Already have it loaded, good
            rec = self.datacache[identifier]
        else:
            # Don't have it, and config says to ignore
            if self.name in self.ignore_sources:
                if self.debug: print(f"       Skipping {source['name']}:{identifier} in collect due to ignore_sources")
                return None

            if self.fetcher.enabled:
                rec = self.fetcher.fetch(identifier)
                if rec is None:
                    if self.debug: print(f"Got no record fetching {self.name}/{identifier}; skipping")
                    return None                    
                if 'identifier' in rec and rec['identifier'] != identifier:
                    # Might have been redirected
                    identifier = rec['identifier']
                if store:
                    self.datacache[identifier] = rec
            else:
                # don't have it, can't fetch it...
                if self.debug: print(f"Cannot fetch {self.name}/{identifier}; skipping")
                return None

        if dataonly:
            return rec

        # Map it
        if rectype is None and 'type' in rec['data']:
            if type(rec['data']) == list:
                rec['data'].remove('Type') # Prefer Language/Material to Type
                rec['data'] = rec['data'][0]
            if rec['data']['type'] in self.configs.ok_record_types:    
                rectype = rec['data']['type']

        rec2 = self.mapper.transform(rec, rectype, reference=reference)

        if rec2 is None:
            if self.debug: print(f"Cannot map {self.name}/{identifier} into Linked Art (as {rectype})")
            return None
        rec2['identifier'] = identifier 
        if reference:
            return rec2

        rec2 = self.mapper.post_mapping(rec2, rectype)
        if rec2:
            if self.validate:
                errs = self.validator.validate(rec2)
                if errs:
                    # err... now what?
                    for error in errs:
                        print(f"  /{'/'.join([str(x) for x in error.absolute_path])} --> {error.message} ")
                    if self.raise_on_error:
                        raise ValueError(errs)
                    else:
                        return None
            if store:
                if self.config['type'] == 'external':
                    try:
                        qrecid = self.configs.make_qua(identifier, rec2['data']['type'])    
                    except:
                        print(f"{rec2['identifier']} in {self.config['name']} has no type?? {rec2['data']}")
                        raise
                    rec2['identifier'] = qrecid
                    self.recordcache[qrecid] = rec2
                else:
                    self.recordcache[identifier] = rec2
        else:
            if self.debug: print(f"Post Mapping killed {rectype} record {self.name}/{identifier}")
        return rec2