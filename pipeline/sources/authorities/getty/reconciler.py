from pipeline.process.base.reconciler import LmdbReconciler


class AatReconciler(LmdbReconciler):
    def extract_names(self, rec):
        # aat_english = self.configs.external['aat']['namespace'] + self.configs.globals_cfg['lang_en']

        ns = self.configs.external["aat"]["namespace"]
        gbls = self.configs.globals_cfg
        aat_primaryName = ns + gbls["primaryName"]

        check_langs = {
            ns + gbls["lang_en"]: 1,  # 820,063
            ns + gbls["lang_fr"]: 2,  # 308,196
            ns + gbls["lang_nl"]: 3,  # 307,119
            ns + gbls["lang_de"]: 4,  # 304,000
            ns + gbls["lang_es"]: 5,  # 259,160
            ns + gbls["lang_ar"]: 6,  # 184,612
            ns + "300389115": 7,  # 172,184
            ns + gbls["lang_zh"]: 8,  # 148,055
        }

        vals = {}
        nms = rec.get("identified_by", [])
        for nm in nms:
            cxns = nm.get("classified_as", [])
            langs = nm.get("language", [])
            if aat_primaryName in [cx["id"] for cx in cxns] and "content" in nm:
                # primary name with value
                val = nm["content"]
                langids = [lng["id"] for lng in langs]
                for lang_id, num in check_langs.items():
                    if lang_id in langids:
                        val = nm["content"].lower().strip()
                        vals[val] = num
                    else:
                        vals[val] = 3
        return vals

    def should_reconcile(self, rec, reconcileType="all"):
        if not LmdbReconciler.should_reconcile(self, rec, reconcileType):
            return False
        if "data" in rec:
            rec = rec["data"]

        if not rec["type"] in [
            "Type",
            "Material",
            "Currency",
            "Language",
            "MeasurementUnit",
        ]:
            return False
        elif "equivalent" in rec:
            # Already reconciled
            eqids = [
                x["id"]
                for x in rec.get("equivalent", [])
                if x["id"].startswith("http://vocab.getty.edu/aat/")
            ]
            return not eqids
        else:
            return True


class UlanReconciler(LmdbReconciler):
    def should_reconcile(self, rec, reconcileType="all"):
        if not LmdbReconciler.should_reconcile(self, rec, reconcileType):
            return False
        if "data" in rec:
            rec = rec["data"]

        if rec["type"] in ["Group", "Person"]:
            # Don't second guess existing equivalent
            eqids = [
                x["id"]
                for x in rec.get("equivalent", [])
                if x["id"].startswith("http://vocab.getty.edu/ulan/")
            ]
            return not eqids
        else:
            return False
