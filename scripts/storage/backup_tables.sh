pg_dump -U rs2668 -F c --clean --no-owner -t activitystreams_data_cache -d rs2668 > /data-io2-2/output/backup/activitystreams_data_cache.pgdump
pg_dump -U rs2668 -F c --clean --no-owner -t dataone_data_cache -d rs2668 > /data-io2-2/output/backup/dataone_data_cache.pgdump
pg_dump -U rs2668 -F c --clean --no-owner -t gbif_data_cache -d rs2668 > /data-io2-2/output/backup/gbif_data_cache.pgdump
pg_dump -U rs2668 -F c --clean --no-owner -t aat_data_cache -d rs2668 > /data-io2-2/output/backup/aat_data_cache.pgdump
pg_dump -U rs2668 -F c --clean --no-owner -t tgn_data_cache -d rs2668 > /data-io2-2/output/backup/tgn_data_cache.pgdump  
pg_dump -U rs2668 -F c --clean --no-owner -t ulan_data_cache -d rs2668 > /data-io2-2/output/backup/ulan_data_cache.pgdump
