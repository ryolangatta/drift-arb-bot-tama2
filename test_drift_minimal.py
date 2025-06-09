#!/usr/bin/env python3
"""
Minimal Drift connection test
"""
import os
import asyncio
import logging
from driftpy.drift_client import DriftClient
from driftpy.keypair import load_keypair
from solana.rpc.async_api import AsyncClient
from solders.keypair import Keypair
from anchorpy import Wallet

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_drift_connection():
    try:
        # Get private key from environment
        private_key_str = os.getenv('SOLANA_DEVNET_PRIVATE_KEY', '')
        
        if not private_key_str:
            logger.error("No SOLANA_DEVNET_PRIVATE_KEY found")
            return False
        
        # Parse private key (handle array format)
        if private_key_str.startswith('['):
            import json
            key_array = json.loads(private_key_str)
            secret_key = bytes(key_array[:32])
            keypair = Keypair.from_seed(secret_key)
        else:
            keypair = load_keypair(private_key_str)
        
        wallet = Wallet(keypair)
        logger.info(f"Wallet loaded: {keypair.pubkey()}")
        
        # Connect to devnet
        connection = AsyncClient("https://api.devnet.solana.com")
        logger.info("Connected to Solana devnet")
        
        # Initialize Drift client
        drift_client = DriftClient(
            connection,
            wallet,
            "devnet"
        )
        
        logger.info("Drift client created")
        
        # Try to subscribe
        await drift_client.subscribe()
        logger.info("✅ Successfully subscribed to Drift!")
        
        await drift_client.unsubscribe()
        await connection.close()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = asyncio.run(test_drift_connection())
    if success:
        print("\n✅ Drift connection test PASSED!")
    else:
        print("\n❌ Drift connection test FAILED!")