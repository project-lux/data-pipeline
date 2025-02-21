from pipeline.process.base.reconciler import LmdbReconciler


class LcshReconciler(LmdbReconciler):
    def should_reconcile(self, rec, reconcileType="all"):
        if not LmdbReconciler.should_reconcile(self, rec, reconcileType):
            return False
        if "data" in rec:
            rec = rec["data"]

        # sh contains geographics
        if not rec["type"] in [
            "Type",
            "Material",
            "Currency",
            "Language",
            "MeasurementUnit",
            "Place",
        ]:
            return False
        elif "equivalent" in rec:
            # Already reconciled
            eqids = [
                x["id"]
                for x in rec.get("equivalent", [])
                if x["id"].startswith("http://id.loc.gov/authorities/")
            ]
            return not eqids
        else:
            return True


class LcnafReconciler(LmdbReconciler):
    def should_reconcile(self, rec, reconcileType="all"):
        if not LmdbReconciler.should_reconcile(self, rec, reconcileType):
            return False
        if "data" in rec:
            rec = rec["data"]

        # FIXME:
        # Person - only return if there are dates
        # Place - Should reconcile bnodes with "(lcnaf) Name" label

        if rec["type"] in ["Group", "Person", "Place"]:
            # Don't second guess existing equivalent
            eqids = [
                x["id"]
                for x in rec.get("equivalent", [])
                if x["id"].startswith("http://id.loc.gov/authorities/")
            ]
            return not eqids
        else:
            return False
