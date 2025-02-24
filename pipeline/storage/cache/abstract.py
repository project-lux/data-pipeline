
class AbstractCache(object):

    def __init__(self, config):
        self.config = config
        self.name = config['name'] + '_' + config['tabletype']
        self.key = "identifier"
        self.metadata_fields = ["insert_time", "record_time", "refresh_time", "valid", "change"]


    def commit(self):
        pass

    def shutdown(self):
        """ Make sure everything is synced and close down all connections """
        raise NotImplementedError()

    def make_threadsafe(self):
        """ Make sure that the cache isn't sharing connections between threads/forks """
        pass

    def len(self):
        """ How many records are in the cache? """
        raise NotImplementedError()

    def latest(self):
        """ Return the datetime of the most recently changed record """
        raise NotImplementedError()

    def len_estimate(self):
        """ Estimate the number of records, for cache backends that have a fast and slow count """
        return self.len()




    def metadata(self, key, field="insert_time", _key_type=None):
        """ Given an identifier and a field, return the value of that field from metadata /about/ the record """
        if _key_type is None:
            _key_type = self.key
        if _key_type == 'yuid' and len(key) != 36:
            raise ValueError(f"{self.name} has UUIDs as keys")
        if not field in self.metadata_fields:
            raise ValueError(f"Unknown metadata field in cache: {field}")


    def set_metadata(self, key, field, value, _key_type=None):
        """ Set a particular field to the given value """
        if _key_type is None:
            _key_type = self.key
        if _key_type == 'yuid' and len(key) != 36:
            raise ValueError(f"{self.name} has UUIDs as keys")
        if not field in self.metadata_fields:
            raise ValueError(f"Unknown metadata field in cache: {field}")



    def iter_records(self):
        pass

    def iter_records_slice(self, mySlice=0, maxSlice=10):
        # use row_number() to partition the results into slices for parallel processing
        if mySlice >= maxSlice:
            raise ValueError(f"{mySlice} cannot be > {maxSlice}")

    def iter_records_since(self, timestamp=None):
        pass


    def iter_keys(self):
        pass

    def iter_keys_slice(self, mySlice=0, maxSlice=10):
        # use row_number() to partition the results into slices for parallel processing
        if mySlice >= maxSlice:
            raise ValueError(f"{mySlice} cannot be > {maxSlice}")

    def iter_keys_slice_mem(self, mySlice=0, maxSlice=10):
        # DON'T use row_number() as it's freaking slow
        if mySlice >= maxSlice:
            raise ValueError(f"{mySlice} cannot be > {maxSlice}")

    def iter_keys_since(self, timestamp=None):
        pass


    def has_item(self, key, _key_type=None, timestamp=None):
        pass

    def get(self, key, _key_type=None):
        # Get a record either by YUID or internal identifier,
        if _key_type is None:
            _key_type = self.key
        if _key_type == 'yuid' and len(key) != 36:
            raise ValueError(f"{self.name} has UUIDs as keys")
        return None

    def set(self, data, identifier=None, yuid=None, format=None, valid=None,
            record_time=None, refresh_time=None, change=None):
        if not identifier and not yuid:
            raise ValueError("Must give YUID or Identifier or both")
        if not type(data) == dict:
            raise ValueError("Data must be a dict()")
        else:
            jdata = Json(data)
        if yuid is not None and not type(yuid) == str:
            yuid = str(yuid)

        insert_time = datetime.datetime.now()
        if record_time is None:
            record_time = insert_time
        if refresh_time is None:
            refresh_time = "9999-12-31T00:00:00Z"


    def delete(self, key, _key_type=None):
        pass

    def clear(self):
        pass



    def start_bulk(self):
        pass

    def set_bulk(self, data, identifier=None, yuid=None, format=None, valid=None, record_time=None, refresh_time=None, change=None):
        pass

    def end_bulk(self):
        pass


    ### Behave like a dict

    def __getitem__(self, what):
        return self.get(what)

    def __setitem__(self, what, value):
        if 'data' in value:
            # given a full record; DO NOT MUTATE IT
            d = value['data']
            params = {}
            if 'identifier' in value and what != value['identifier']:
                raise ValueError("Record's identifier value and key given to set are different")
            for v in ['identifier', 'yuid', 'format', 'valid', 'change', 'record_time']:
                if v in value:
                    params[v] = value[v]
            return self.set(d, **params)
        else:
            # given only the json
            if self.key == 'identifier':
                return self.set(value, identifier=what)
            elif self.key == 'yuid':
                return self.set(value, yuid=what)

    def __delitem__(self, what):
        return self.delete(what)

    def __contains__(self, what):
        return self.has_item(what)

    def __len__(self):
        return self.len()

    def __bool__(self):
        # Don't let it go through to len
        # and warn that the code shouldn't do this
        print(f"*** code somewhere is asking for a cache as a boolean value and shouldn't ***")
        return True

