import requests
import pandas as pd
from pkg_resources import resource_string
from functools import partial
from itertools import chain
from contextlib import closing
import json


class ResourceGetter(object):
    def __init__(self, r):
        self.r = r

    def __get__(self, inst, cls):
        return partial(inst.get, self.r)

class Sdmx:
    __agencies = json.loads(resource_string('srkimsdmx', 'agencies.json'))
    __resources = ['dataflow', 'datastructure', 'data', 'categoryscheme', 'codelist', 'conceptscheme']

    @classmethod
    def _make_get_wrappers(cls):
        for r in cls.__resources:
            setattr(cls, r, ResourceGetter(r))

    def __init__(self, agency=''):
        self.agency = agency.upper()
        self.client = ''

    def get(self, resource='', resource_id='', version=None, key=''):
        if resource == 'data' and isinstance(key, dict):
            # select validation method based on agency capabilities
            if self.__agencies[self.agency].get('supports_series_keys_only'):
                key = self._make_key_from_series(resource_id, key)
            else:
                key = self._make_key_from_dsd(resource_id, key)

        parts = [self.__agencies[self.agency]['url'],
                 resource,
                 self.__agencies[self.agency]['id'],
                 resource_id, version, key]
        base_url = '/'.join(filter(None, parts))

    def _make_key_from_dsd(self, flow_id, key):

        dataflow = self.get('dataflow', flow_id)
        dsd_id = dataflow.msg.dataflow[flow_id].structure.id
        dsd_resp = self.get('datastructure', dsd_id)
        dsd = dsd_resp.msg.datastructure[dsd_id]

        dimensions = [d for d in dsd.dimensions.aslist() if d.id not in
                      ['TIME', 'TIME_PERIOD']]
        dim_names = [d.id for d in dimensions]

        try:
            constraint_l = [c for c in dataflow.constraint.aslist()
                            if c.constraint_attachment.id == flow_id]
            if constraint_l:
                constraint = constraint_l[0]
        except:
            constraint = None

        invalid = [d for d in key.keys()
                   if d not in dim_names]
        if invalid:
            raise ValueError(
                'Invalid dimension name {0}, allowed are: {1}'.format(invalid, dim_names))

        parts = []

        for d in dimensions:
            try:
                values = key[d.id]
                values_l = values.split('+')
                codelist = d.local_repr.enum
                codes = codelist.keys()
                invalid = [v for v in values_l if v not in codes]
                if invalid:
                    raise ValueError("'{0}' is not in codelist for dimension '{1}: {2}'".
                                     format(invalid, d.id, codes))
                # Check if values are in Contentconstraint if present
                if constraint:
                    try:
                        invalid = [
                            v for v in values_l if (d.id, v) not in constraint]
                        if invalid:
                            raise ValueError("'{0}' out of content_constraint for '{1}'.".
                                             format(invalid, d.id))
                    except NotImplementedError:
                        pass
                part = values
            except KeyError:
                part = ''
            parts.append(part)
        return '.'.join(parts)

    def series_keys(self, flow_id):
        res = self.data(flow_id, params={'detail': 'serieskeysonly'})
        l = list(s.key for s in res.data.series)
        df = pd.DataFrame(l, columns=l[0]._fields, dtype='category')
        return df

    def _make_key_from_series(self, flow_id, key):

        all_keys = self.series_keys(flow_id)
        dim_names = list(all_keys)

        invalid = [d for d in key
                   if d not in dim_names]
        if invalid:
            raise ValueError(
                'Invalid dimension name {0}, allowed are: {1}'.format(invalid, dim_names))

        key = {k: v.split('+') if '+' in v else v for k, v in key.items()}

        key_l = {k: [v] if isinstance(v, str) else v
                 for k, v in key.items()}

        invalid = list(chain.from_iterable((((k, v) for v in vl if v not in all_keys[k].values)
                                            for k, vl in key_l.items())))
        if invalid:
            raise ValueError("The following dimension values are invalid: {0}".
                             format(invalid))

        for k, v in key_l.items():
            key_l[k] = '+'.join(v)

        parts = [key_l.get(name, '') for name in dim_names]
        return '.'.join(parts)


class Rest:
    def __init__(self, cfg):
        default_cfg = dict(stream=True, timeout=30)
        #for it in default_cfg.items():
        #    cfg.setdefault(*it)
        #self.config = cfg

    def req(self, url, params={}, headers={}):
        with closing(requests.get(url, params=params)) as res:
            j = res.json()
            print(j)


Sdmx('ESTAT').get('dataflow', '')