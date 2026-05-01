from fastapi import FastAPI, HTTPException
from data_plane.transformation.silver_transformer import SilverTransformer
from data_plane.transformation.gold_aggregator import GoldAggregator
import logging

app = FastAPI()
log = logging.getLogger(__name__)

@app.post("/transform/silver/{source_id}")
def run_silver(source_id: str):
    try:
        log.info(f"Starting Silver transformation for {source_id}")
        transformer = SilverTransformer(source_id)
        result = transformer.transform()
        log.info(f"Silver transformation completed for {source_id}")
        return result.__dict__
    except Exception as e:
        log.error(f"Silver transformation failed for {source_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/transform/gold")
def run_gold():
    try:
        log.info("Starting Gold aggregation")
        aggregator = GoldAggregator()
        result = aggregator.run()
        log.info("Gold aggregation completed")
        return result
    except Exception as e:
        log.error(f"Gold aggregation failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))