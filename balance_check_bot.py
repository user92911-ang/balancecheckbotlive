import asyncio
import logging
import os
import re
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Dict, List, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict
import time
import random

import aiohttp
from fastapi import FastAPI
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

# Rate limiting for users
user_last_request = {}
active_user_requests = defaultdict(int)

# Global session management
global_session: Optional[aiohttp.ClientSession] = None

# --- Standard Configuration ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Load Secrets from Environment Variables ---
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")

# --- Bot Configuration ---
TIP_ADDRESS = "0x50A0d7Fb9f64e908688443cB94c3971705599d79"
MAX_CONCURRENT_REQUESTS = 12
REQUEST_TIMEOUT = 8
MAX_RETRIES = 3  # Increased retries for better reliability
BATCH_SIZE = 6
USER_RATE_LIMIT = 2
RPC_FAILURE_COOLDOWN = 300

# RPC health tracking
rpc_failure_tracker = defaultdict(lambda: {"failures": 0, "last_failure": 0})

# Precomputed decimals to avoid extra RPC calls
TOKEN_DECIMALS = {
    'USDT': 6,
    'USDC': 6,
    # BSC-specific overrides for correct decimals
    'bsc_USDT': 18,
    'bsc_USDC': 18,
}

@dataclass
class BalanceCheck:
    chain_id: str
    address: str
    symbol: str
    contract: Optional[str] = None
    is_native: bool = True

# --- Enhanced Chain Configuration with Better RPC Management ---
CHAINS = {
    'ethereum': {
        'name': 'Ethereum Mainnet', 
        'symbol': 'ETH', 
        'rpcs': [
            'https://eth.llamarpc.com',
            'https://rpc.ankr.com/eth',
            'https://ethereum.publicnode.com',
            'https://eth-mainnet.public.blastapi.io',
            'https://rpc.flashbots.net',
            'https://eth.meowrpc.com',
            'https://eth-mainnet.g.alchemy.com/v2/demo'
        ]
    },
    'base': {
        'name': 'Base', 
        'symbol': 'ETH', 
        'rpcs': [
            'https://mainnet.base.org',
            'https://base.publicnode.com',
            'https://base-mainnet.public.blastapi.io',
            'https://base.llamarpc.com',
            'https://base.meowrpc.com',
            'https://rpc.ankr.com/base',
            'https://base-rpc.publicnode.com'
        ]
    },
    'arbitrum': {
        'name': 'Arbitrum', 
        'symbol': 'ETH', 
        'rpcs': [
            'https://arb1.arbitrum.io/rpc',
            'https://rpc.ankr.com/arbitrum',
            'https://arbitrum.publicnode.com',
            'https://arbitrum-one.public.blastapi.io',
            'https://arbitrum.llamarpc.com'
        ]
    },
    'optimism': {
        'name': 'Optimism', 
        'symbol': 'ETH', 
        'rpcs': [
            'https://mainnet.optimism.io',
            'https://rpc.ankr.com/optimism',
            'https://optimism.publicnode.com',
            'https://optimism.llamarpc.com'
        ]
    },
    'polygon': {
        'name': 'Polygon', 
        'symbol': 'MATIC', 
        'rpcs': [
            'https://polygon-rpc.com',
            'https://rpc.ankr.com/polygon',
            'https://polygon.publicnode.com',
            'https://polygon-mainnet.public.blastapi.io',
            'https://polygon.llamarpc.com'
        ]
    },
    'bsc': {
        'name': 'BSC', 
        'symbol': 'BNB', 
        'rpcs': [
            'https://bsc-dataseed.binance.org',
            'https://rpc.ankr.com/bsc',
            'https://bnb.publicnode.com',
            'https://bsc-mainnet.public.blastapi.io',
            'https://bsc.publicnode.com'
        ]
    },
    'ink': {
        'name': 'Ink', 
        'symbol': 'ETH', 
        'rpcs': ['https://rpc-gel.inkonchain.com']
    },
    'unichain': {
        'name': 'Unichain', 
        'symbol': 'ETH', 
        'rpcs': ['https://mainnet.unichain.org']
    },
    'abstract': {
        'name': 'Abstract', 
        'symbol': 'ETH', 
        'rpcs': ['https://api.mainnet.abs.xyz']
    },
}

ERC20_CONTRACTS = {
    'ethereum': {'USDT': '0xdac17f958d2ee523a2206206994597c13d831ec7', 'USDC': '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'},
    'base': {'USDT': '0xfde4C96c8593536E31F229EA8f37b2ADa2699bb2', 'USDC': '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'},
    'arbitrum': {'USDC': '0xaf88d065e77c8cC2239327C5EDb3A432268e5831', 'USDT': '0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9'},
    'optimism': {'USDC': '0x0b2c639c533813f4aa9d7837caf62653d097ff85', 'USDT': '0x94b008aa00579c1307b0ef2c499ad98a8ce58e58'},
    'polygon': {'USDC': '0x3c499c542cef5e3811e1192ce70d8cc03d5c3359', 'USDT': '0xc2132D05D31c914a87C6611C10748AEb04B58e8F'},
    'bsc': {'USDT': '0x55d398326f99059ff775485246999027b3197955', 'USDC': '0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d'},
    'ink': {'USDT': '0x0200C29006150606B650577BBE7B6248F58470c1'},
    'unichain': {'USDT': '0x9151434b16b9763660705744891fA906F660EcC5'},
    'abstract': {'USDC': '0x07865c6E87B9F70255377e024ace6630C1Eaa37F', 'USDT': '0x0200C29006150606B650577BBE7B6248F58470c1'},
}

class BalanceFetchError(Exception):
    """Custom exception for balance fetching errors"""
    pass

# --- Global Session Management ---
async def get_global_session() -> aiohttp.ClientSession:
    """Get or create the global session"""
    global global_session
    
    if global_session is None or global_session.closed:
        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=15,
            ttl_dns_cache=600,
            use_dns_cache=True,
            keepalive_timeout=30,
            enable_cleanup_closed=True
        )
        
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT, connect=5)
        global_session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={'User-Agent': 'CryptoBalanceBot/1.0'}
        )
        logger.info("Created new global aiohttp session")
    
    return global_session

def is_rpc_healthy(rpc_url: str) -> bool:
    """Check if RPC is healthy based on recent failures"""
    current_time = time.time()
    tracker = rpc_failure_tracker[rpc_url]
    
    if tracker["failures"] == 0:
        return True
    
    if current_time - tracker["last_failure"] > RPC_FAILURE_COOLDOWN:
        tracker["failures"] = 0
        return True
    
    return tracker["failures"] < 3

def mark_rpc_failure(rpc_url: str):
    """Mark an RPC as having failed"""
    tracker = rpc_failure_tracker[rpc_url]
    tracker["failures"] += 1
    tracker["last_failure"] = time.time()
    logger.warning(f"RPC failure tracked for {rpc_url} (total: {tracker['failures']})")

def get_healthy_rpcs(chain_id: str) -> List[str]:
    """Get list of healthy RPCs for a chain, with randomization"""
    all_rpcs = CHAINS[chain_id]['rpcs']
    healthy_rpcs = [rpc for rpc in all_rpcs if is_rpc_healthy(rpc)]
    
    if not healthy_rpcs:
        logger.warning(f"No healthy RPCs for {chain_id}, resetting failure trackers")
        for rpc in all_rpcs:
            rpc_failure_tracker[rpc]["failures"] = 0
        healthy_rpcs = all_rpcs
    
    random.shuffle(healthy_rpcs)
    return healthy_rpcs

# --- Optimized RPC and Balance Fetching ---

async def get_eth_price(session: aiohttp.ClientSession) -> float:
    """Fetch ETH price from CoinGecko with better error handling"""
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {"ids": "ethereum", "vs_currencies": "usd"}
    if COINGECKO_API_KEY:
        params['x_cg_demo_api_key'] = COINGECKO_API_KEY
    
    try:
        async with session.get(url, params=params, timeout=5) as r:
            if r.status == 200:
                data = await r.json()
                return data.get('ethereum', {}).get('usd', 0.0)
            elif r.status == 429:
                logger.warning("CoinGecko rate limited")
    except Exception as e:
        logger.warning(f"ETH price fetch failed: {e}")
    return 0.0

async def make_batch_rpc_call_with_retry(session: aiohttp.ClientSession, 
                                       chain_id: str, 
                                       payloads: List[dict], 
                                       context: str) -> List[Optional[dict]]:
    """Make batch RPC call with intelligent retry and RPC selection"""
    healthy_rpcs = get_healthy_rpcs(chain_id)
    last_error = None
    
    for rpc_attempt, rpc_url in enumerate(healthy_rpcs[:MAX_RETRIES]):
        try:
            if len(payloads) == 1:
                # Single call
                async with session.post(rpc_url, json=payloads[0]) as response:
                    if response.status == 200:
                        data = await response.json()
                        if 'error' not in data:
                            return [data]
                        else:
                            last_error = data.get('error')
                            logger.debug(f"RPC error on {rpc_url}: {last_error}")
                    elif response.status == 429:
                        logger.warning(f"Rate limited on {rpc_url} for {context}")
                        mark_rpc_failure(rpc_url)
                        continue
                    else:
                        mark_rpc_failure(rpc_url)
                        continue
            else:
                # Batch call
                async with session.post(rpc_url, json=payloads) as response:
                    if response.status == 200:
                        data = await response.json()
                        if isinstance(data, list):
                            # Process each result, keeping track of successes
                            results = []
                            for item in data:
                                if 'error' not in item:
                                    results.append(item)
                                else:
                                    results.append(None)
                            
                            # If we got at least some successful results, return them
                            success_count = sum(1 for r in results if r is not None)
                            if success_count > 0:
                                logger.debug(f"Batch call to {rpc_url} succeeded with {success_count}/{len(results)} results")
                                return results
                        else:
                            if 'error' not in data:
                                return [data]
                    elif response.status == 429:
                        logger.warning(f"Rate limited on {rpc_url} for batch {context}")
                        mark_rpc_failure(rpc_url)
                        continue
                    else:
                        mark_rpc_failure(rpc_url)
                        continue
                        
        except asyncio.TimeoutError:
            logger.warning(f"Timeout for {context} on {rpc_url}")
            mark_rpc_failure(rpc_url)
            continue
        except Exception as e:
            logger.warning(f"RPC call failed for {context} on {rpc_url}: {e}")
            mark_rpc_failure(rpc_url)
            last_error = str(e)
            continue
    
    logger.error(f"All RPCs failed for {context} on {chain_id}. Last error: {last_error}")
    return [None] * len(payloads)

def parse_balance_result(result_hex: str, decimals: int = 18) -> float:
    """Safely parse a hex balance result"""
    try:
        # Handle empty or '0x' results
        if not result_hex or result_hex == '0x' or result_hex == '0x0':
            return 0.0
        
        # Remove '0x' prefix if present
        if result_hex.startswith('0x'):
            result_hex = result_hex[2:]
        
        # If still empty after removing prefix, return 0
        if not result_hex:
            return 0.0
        
        # Convert hex to int and then to decimal
        balance_raw = int(result_hex, 16)
        return balance_raw / (10 ** decimals)
    except (ValueError, TypeError) as e:
        logger.debug(f"Error parsing balance result '{result_hex}': {e}")
        return 0.0

async def process_balance_batch(session: aiohttp.ClientSession, 
                              checks: List[BalanceCheck], 
                              semaphore: asyncio.Semaphore) -> List[Tuple[str, str, float]]:
    """Process a batch of balance checks for the same chain with better error handling"""
    if not checks:
        return []
    
    async with semaphore:
        chain_id = checks[0].chain_id
        results = []
        
        # Group by type (native vs ERC20)
        native_checks = [c for c in checks if c.is_native]
        erc20_checks = [c for c in checks if not c.is_native]
        
        # Process native balances in batch
        if native_checks:
            native_payloads = []
            for i, check in enumerate(native_checks):
                native_payloads.append({
                    "jsonrpc": "2.0",
                    "method": "eth_getBalance",
                    "params": [check.address, "latest"],
                    "id": i + 1
                })
            
            native_responses = await make_batch_rpc_call_with_retry(
                session, chain_id, native_payloads, f"native batch on {chain_id}"
            )
            
            for check, response in zip(native_checks, native_responses):
                if response and 'result' in response:
                    balance = parse_balance_result(response['result'], 18)
                    results.append((check.chain_id, check.symbol, balance))
                else:
                    # For failed native balance checks, try individual retry
                    logger.warning(f"Native balance check failed for {check.address} on {chain_id}, retrying individually")
                    individual_response = await make_batch_rpc_call_with_retry(
                        session, chain_id, 
                        [{
                            "jsonrpc": "2.0",
                            "method": "eth_getBalance",
                            "params": [check.address, "latest"],
                            "id": 1
                        }],
                        f"individual native for {check.address[:10]}... on {chain_id}"
                    )
                    if individual_response[0] and 'result' in individual_response[0]:
                        balance = parse_balance_result(individual_response[0]['result'], 18)
                        results.append((check.chain_id, check.symbol, balance))
                    else:
                        results.append((check.chain_id, check.symbol, 0.0))
        
        # Process ERC20 balances in batch
        if erc20_checks:
            erc20_payloads = []
            for i, check in enumerate(erc20_checks):
                erc20_payloads.append({
                    "jsonrpc": "2.0",
                    "method": "eth_call",
                    "params": [{
                        "to": check.contract,
                        "data": f"0x70a08231{check.address[2:].zfill(64)}"
                    }, "latest"],
                    "id": len(native_checks) + i + 1
                })
            
            erc20_responses = await make_batch_rpc_call_with_retry(
                session, chain_id, erc20_payloads, f"ERC20 batch on {chain_id}"
            )
            
            for check, response in zip(erc20_checks, erc20_responses):
                if response and 'result' in response:
                    # Fix for BSC decimal issue
                    decimals_key = f"{check.chain_id}_{check.symbol}" if check.chain_id == 'bsc' else check.symbol
                    decimals = TOKEN_DECIMALS.get(decimals_key, 18)
                    balance = parse_balance_result(response['result'], decimals)
                    results.append((check.chain_id, check.symbol, balance))
                else:
                    results.append((check.chain_id, check.symbol, 0.0))
        
        return results

async def verify_critical_balances(session: aiohttp.ClientSession, addresses: List[str], 
                                  initial_balances: Dict) -> Dict:
    """Re-verify balances for critical chains like Base with more robust checking"""
    logger.info("Running verification pass for critical chains...")
    
    critical_chains = ['base', 'ethereum', 'arbitrum']  # Chains to double-check
    verification_checks = []
    
    for addr in addresses:
        for chain_id in critical_chains:
            if chain_id in CHAINS:
                verification_checks.append(BalanceCheck(
                    chain_id=chain_id,
                    address=addr,
                    symbol=CHAINS[chain_id]['symbol'],
                    is_native=True
                ))
    
    # Use a smaller batch size for verification
    semaphore = asyncio.Semaphore(6)
    tasks = []
    
    for i in range(0, len(verification_checks), 2):  # Smaller batches of 2
        batch = verification_checks[i:i + 2]
        tasks.append(process_balance_batch(session, batch, semaphore))
    
    try:
        batch_results = await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True),
            timeout=20
        )
        
        # Compare and update with verified results
        verified_balances = initial_balances.copy()
        
        for batch_result in batch_results:
            if isinstance(batch_result, Exception):
                continue
                
            for chain_id, symbol, balance in batch_result:
                if balance > 0.000001:  # Only update if we found a non-zero balance
                    if chain_id not in verified_balances:
                        verified_balances[chain_id] = {}
                    
                    # Take the maximum of initial and verified balance
                    current = verified_balances[chain_id].get(symbol, 0)
                    if balance > current:
                        logger.info(f"Updated {chain_id} {symbol} balance from {current} to {balance}")
                        verified_balances[chain_id][symbol] = balance
        
        return verified_balances
        
    except asyncio.TimeoutError:
        logger.warning("Verification pass timed out, using initial results")
        return initial_balances

async def get_all_asset_balances_optimized(session: aiohttp.ClientSession, addresses: List[str]) -> Dict:
    """Optimized balance fetching with verification pass"""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    # Generate all balance checks
    all_checks = []
    
    for addr in addresses:
        for chain_id, chain_info in CHAINS.items():
            # Native balance check
            all_checks.append(BalanceCheck(
                chain_id=chain_id,
                address=addr,
                symbol=chain_info['symbol'],
                is_native=True
            ))
            
            # ERC20 balance checks
            if chain_id in ERC20_CONTRACTS:
                for symbol, contract in ERC20_CONTRACTS[chain_id].items():
                    all_checks.append(BalanceCheck(
                        chain_id=chain_id,
                        address=addr,
                        symbol=symbol,
                        contract=contract,
                        is_native=False
                    ))
    
    logger.info(f"Generated {len(all_checks)} balance checks for {len(addresses)} addresses")
    
    # Group checks by chain for batch processing
    chain_groups = defaultdict(list)
    for check in all_checks:
        chain_groups[check.chain_id].append(check)
    
    # Process each chain's checks in batches
    tasks = []
    for chain_id, checks in chain_groups.items():
        # Split into smaller batches
        for i in range(0, len(checks), BATCH_SIZE):
            batch = checks[i:i + BATCH_SIZE]
            tasks.append(process_balance_batch(session, batch, semaphore))
    
    logger.info(f"Processing {len(tasks)} batches across {len(chain_groups)} chains")
    
    # Execute all batches concurrently with timeout
    try:
        batch_results = await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True),
            timeout=30
        )
    except asyncio.TimeoutError:
        logger.error("Balance fetching timed out after 30 seconds")
        return {}
    
    # Aggregate results
    aggregated = {}
    total_balances = 0
    
    for batch_result in batch_results:
        if isinstance(batch_result, Exception):
            logger.error(f"Batch failed: {batch_result}")
            continue
            
        for chain_id, symbol, balance in batch_result:
            if balance and balance > 0.000001:  # Filter out dust
                if chain_id not in aggregated:
                    aggregated[chain_id] = {}
                if symbol not in aggregated[chain_id]:
                    aggregated[chain_id][symbol] = 0
                aggregated[chain_id][symbol] += balance
                total_balances += 1
    
    logger.info(f"Initial pass found {total_balances} non-zero balances across {len(aggregated)} chains")
    
    # Run verification pass for critical chains
    verified_balances = await verify_critical_balances(session, addresses, aggregated)
    
    return verified_balances

# --- Address Parsing and ENS Resolution ---

async def resolve_ens_to_address(session: aiohttp.ClientSession, name: str) -> str | None:
    """Resolve ENS name to address with timeout and retry"""
    try:
        async with session.get(f"https://api.ensideas.com/ens/resolve/{name.lower()}", 
                             timeout=8) as response:
            if response.status == 200:
                data = await response.json()
                address = data.get("address")
                if address:
                    logger.info(f"Resolved ENS {name} to {address}")
                return address
    except Exception as e:
        logger.warning(f"ENS resolution error for {name}: {e}")
    return None

async def parse_and_resolve_addresses(session: aiohttp.ClientSession, text: str) -> List[str]:
    """Parse and resolve addresses with improved validation and common address formats"""
    address_pattern = r'0x[a-fA-F0-9]{40}'
    ens_pattern = r'[a-zA-Z0-9-]+\.eth'
    
    context_address_pattern = r'(?:address|wallet|addr|account)[:=\s]+0x[a-fA-F0-9]{40}'
    
    found_addresses = set()
    found_ens_names = set()
    
    found_addresses.update(addr.lower() for addr in re.findall(address_pattern, text, re.IGNORECASE))
    found_ens_names.update(name.lower() for name in re.findall(ens_pattern, text, re.IGNORECASE))
    
    contextual_matches = re.findall(context_address_pattern, text, re.IGNORECASE)
    for match in contextual_matches:
        addr_match = re.search(r'0x[a-fA-F0-9]{40}', match, re.IGNORECASE)
        if addr_match:
            found_addresses.add(addr_match.group().lower())
    
    logger.info(f"Found {len(found_addresses)} addresses and {len(found_ens_names)} ENS names")
    
    if found_ens_names:
        try:
            resolved_addresses = await asyncio.wait_for(
                asyncio.gather(
                    *(resolve_ens_to_address(session, name) for name in found_ens_names),
                    return_exceptions=True
                ),
                timeout=15
            )
            
            for addr in resolved_addresses:
                if isinstance(addr, str) and addr:
                    found_addresses.add(addr.lower())
        except asyncio.TimeoutError:
            logger.warning("ENS resolution timed out")
    
    final_addresses = list(found_addresses)
    logger.info(f"Final address list: {len(final_addresses)} addresses")
    return final_addresses

# --- Command Handlers ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    image_url = "https://i.ibb.co/qMZjmJ9V/eth2.png"
    welcome_message = f"[​]({image_url})"
    welcome_message += """
    🤖 **Crypto Balance Bot** ⚡

I'll help you check native and stablecoin balances across multiple EVM chains! I also resolve `.eth` names.

**Commands:**
/start - Show this help message
/about - Info & support the creator
/debug - Test RPC endpoint speeds (admin only)
/health - Show system health status

**Supported Chains:**
• Ethereum Mainnet
• Base
• Ink  
• Abstract
• Arbitrum
• Unichain
• Polygon
• Optimism
• BSC

**Usage:**
1. Paste your wallet addresses or `.eth` names.
2. You can paste up to 200.
3. I'll find all native and stablecoin balances across all supported chains and return a summary.

**Example:**
0x742d35Cc6634C0532925a3b8D5C9E49C7F59c2c4
vitalik.eth

⚡ **Enhanced with verification passes for improved accuracy!**
"""
    await update.message.reply_text(welcome_message, parse_mode='Markdown', disable_web_page_preview=False)

async def about_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    about_message = f"Tip Jar (ETH/EVM):\n`{TIP_ADDRESS}`"
    await update.message.reply_text(about_message, parse_mode='Markdown')

async def health_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show system health status"""
    health_info = []
    
    global global_session
    if global_session and not global_session.closed:
        health_info.append("✅ Global session: Active")
    else:
        health_info.append("❌ Global session: Inactive")
    
    total_rpcs = 0
    unhealthy_rpcs = 0
    
    for chain_id, chain_info in CHAINS.items():
        healthy_count = 0
        total_chain_rpcs = len(chain_info['rpcs'])
        total_rpcs += total_chain_rpcs
        
        for rpc in chain_info['rpcs']:
            if is_rpc_healthy(rpc):
                healthy_count += 1
            else:
                unhealthy_rpcs += 1
        
        status = "✅" if healthy_count == total_chain_rpcs else "⚠️" if healthy_count > 0 else "❌"
        health_info.append(f"{status} {chain_info['name']}: {healthy_count}/{total_chain_rpcs} RPCs healthy")
    
    healthy_rpcs = total_rpcs - unhealthy_rpcs
    overall_health = "✅ Excellent" if unhealthy_rpcs == 0 else "⚠️ Degraded" if unhealthy_rpcs < total_rpcs * 0.3 else "❌ Poor"
    
    health_message = f"🏥 **System Health Status**\n\n"
    health_message += f"**Overall Health:** {overall_health}\n"
    health_message += f"**RPC Status:** {healthy_rpcs}/{total_rpcs} healthy\n\n"
    health_message += "\n".join(health_info)
    
    if unhealthy_rpcs > 0:
        health_message += f"\n\n💡 **Note:** Unhealthy RPCs will recover automatically after {RPC_FAILURE_COOLDOWN//60} minutes"
    
    await update.message.reply_text(health_message, parse_mode='Markdown')

async def debug_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Test RPC endpoint speeds for debugging"""
    status_message = await update.message.reply_text("🔧 Testing RPC endpoint speeds...")
    
    test_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
    results = []
    
    session = await get_global_session()
    
    for chain_id, chain_info in CHAINS.items():
        chain_results = []
        healthy_rpcs = get_healthy_rpcs(chain_id)
        
        for rpc_url in healthy_rpcs[:3]:
            start_time = asyncio.get_event_loop().time()
            
            payload = {
                "jsonrpc": "2.0",
                "method": "eth_getBalance", 
                "params": [test_address, "latest"],
                "id": 1
            }
            
            try:
                async with session.post(rpc_url, json=payload, timeout=10) as response:
                    end_time = asyncio.get_event_loop().time()
                    response_time = end_time - start_time
                    
                    if response.status == 200:
                        data = await response.json()
                        if 'result' in data:
                            status = f"✅ {response_time:.2f}s"
                        else:
                            status = f"❌ RPC Error {response_time:.2f}s"
                    else:
                        status = f"❌ HTTP {response.status} {response_time:.2f}s"
                            
            except asyncio.TimeoutError:
                status = "⏰ Timeout >10s"
            except Exception as e:
                status = f"❌ Error: {str(e)[:20]}"
            
            rpc_name = rpc_url.split('//')[1].split('/')[0]
            
            health_status = "🟢" if is_rpc_healthy(rpc_url) else "🔴"
            chain_results.append(f"  {health_status} {rpc_name}: {status}")
        
        results.append(f"**{chain_info['name']}:**\n" + "\n".join(chain_results))
    
    debug_message = "🔧 **RPC Endpoint Speed Test:**\n\n" + "\n\n".join(results)
    debug_message += f"\n\n💡 **Legend:**\n🟢 = Healthy RPC\n🔴 = Recently failed RPC"
    debug_message += f"\n\n**Analysis:** Look for endpoints consistently >3s or with errors."
    
    await status_message.edit_text(debug_message, parse_mode='Markdown')

async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    current_time = time.time()
    
    if user_id in user_last_request:
        time_since_last = current_time - user_last_request[user_id]
        if time_since_last < 15:
            await update.message.reply_text(f"⏳ Please wait {15 - int(time_since_last)} seconds before your next request.")
            return
    
    if active_user_requests[user_id] >= USER_RATE_LIMIT:
        await update.message.reply_text("⚠️ You have too many active requests. Please wait for them to complete.")
        return
    
    user_last_request[user_id] = current_time
    active_user_requests[user_id] += 1
    
    try:
        start_time = asyncio.get_event_loop().time()
        status_message = await update.message.reply_text("🔍 Resolving addresses...")
        
        session = await get_global_session()
        
        addresses = await parse_and_resolve_addresses(session, update.message.text)
        if not addresses:
            await status_message.edit_text("❌ I didn't find any valid addresses or `.eth` names.")
            return

        if len(addresses) > 200:
            await status_message.edit_text("❌ Too many addresses! Please limit to 200 addresses maximum.")
            return

        await status_message.edit_text(f"✅ Found {len(addresses)} unique address(es).\n⚡ Fetching balances with verification pass...")
        
        balance_task = get_all_asset_balances_optimized(session, addresses)
        price_task = get_eth_price(session)
        
        try:
            aggregated_balances, eth_price = await asyncio.wait_for(
                asyncio.gather(balance_task, price_task),
                timeout=45
            )
        except asyncio.TimeoutError:
            await status_message.edit_text("⏰ Request timed out. The RPCs might be slow right now. Please try again in a few minutes.")
            return

        if not aggregated_balances:
            await status_message.edit_text("❌ No balances found for the provided addresses.")
            return

        grand_totals = {}
        total_chains_with_balances = len(aggregated_balances)
        
        for chain_id, tokens in aggregated_balances.items():
            for symbol, balance in tokens.items():
                if symbol not in grand_totals:
                    grand_totals[symbol] = 0
                grand_totals[symbol] += balance

        execution_time = asyncio.get_event_loop().time() - start_time
        final_message = f"📊 **Balance Summary for {len(addresses)} address(es)** (⚡ {execution_time:.1f}s)\n\n"
        
        chain_items = list(aggregated_balances.items())
        try:
            chain_items.sort(key=lambda x: (
                -x[1].get('ETH', 0),
                x[0]
            ))
        except:
            chain_items.sort()
        
        for chain_id, tokens in chain_items:
            chain_name = CHAINS[chain_id]['name']
            token_lines = []
            
            token_items = list(tokens.items())
            token_items.sort(key=lambda x: (
                0 if x[0] in ['ETH'] else 1 if x[0] in ['USDC', 'USDT'] else 2,
                -x[1]
            ))
            
            for symbol, balance in token_items:
                if balance >= 0.000001:
                    if symbol in ['USDC', 'USDT']:
                        formatted_balance = f"{balance:,.2f}"
                    elif symbol in ['ETH', 'BNB', 'MATIC']:
                        formatted_balance = f"{balance:,.6f}".rstrip('0').rstrip('.')
                    else:
                        formatted_balance = f"{balance:,.6f}".rstrip('0').rstrip('.')
                    
                    token_lines.append(f"{formatted_balance} {symbol}")
            
            if token_lines:
                final_message += f"• **{chain_name}:** {', '.join(token_lines)}\n"
        
        if grand_totals:
            final_message += "\n" + "="*30 + "\n**🎯 GRAND TOTALS ACROSS ALL CHAINS:**\n"
            
            total_items = list(grand_totals.items())
            total_items.sort(key=lambda x: (
                0 if x[0] in ['ETH'] else 1 if x[0] in ['USDC', 'USDT'] else 2,
                -x[1]
            ))
            
            for symbol, total in total_items:
                if total >= 0.000001:
                    if symbol in ['USDC', 'USDT']:
                        formatted_total = f"{total:,.2f}"
                    else:
                        formatted_total = f"{total:,.6f}".rstrip('0').rstrip('.')
                    final_message += f"**{symbol}:** {formatted_total}\n"

            portfolio_usd = 0
            if 'ETH' in grand_totals and grand_totals['ETH'] > 0 and eth_price > 0:
                eth_usd = grand_totals['ETH'] * eth_price
                portfolio_usd += eth_usd
                final_message += f"\n💰 **ETH Value:** `${eth_usd:,.2f}` (@ `${eth_price:,.2f}/ETH`)\n"
            
            stablecoin_total = grand_totals.get('USDC', 0) + grand_totals.get('USDT', 0)
            if stablecoin_total > 0:
                portfolio_usd += stablecoin_total
                final_message += f"💵 **Stablecoin Value:** `${stablecoin_total:,.2f}`\n"
            
            if portfolio_usd > 0:
                final_message += f"🏦 **Total Portfolio Value:** `${portfolio_usd:,.2f}`\n"
        
        final_message += f"\n✅ Found balances on {total_chains_with_balances} chain(s)"
        
        if execution_time > 10:
            final_message += f"\n⚠️ Response was slower than usual due to RPC limitations"
        
        await status_message.edit_text(final_message, parse_mode='Markdown', disable_web_page_preview=True)
        
        logger.info(f"Completed balance check for {len(addresses)} addresses in {execution_time:.1f}s. "
                   f"Found balances on {total_chains_with_balances} chains. User: {user_id}")

    except Exception as e:
        logger.error(f"Error in balance_command for user {user_id}: {e}", exc_info=True)
        error_message = "❌ An error occurred while fetching balances. Please try again."
        
        if "timeout" in str(e).lower():
            error_message += "\n⏰ This appears to be a timeout. RPCs might be slow right now."
        elif "rate" in str(e).lower() or "429" in str(e):
            error_message += "\n⚠️ Rate limited. Please wait a few minutes before trying again."
        
        try:
            await status_message.edit_text(error_message)
        except:
            await update.message.reply_text(error_message)
    
    finally:
        active_user_requests[user_id] = max(0, active_user_requests[user_id] - 1)

# --- Lifespan Manager & Web Server Setup ---
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    if not TELEGRAM_TOKEN:
        logger.critical("CRITICAL: TELEGRAM_TOKEN not set.")
        yield
        return
        
    await get_global_session()
    
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("about", about_command))
    application.add_handler(CommandHandler("debug", debug_command))
    application.add_handler(CommandHandler("health", health_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, balance_command))
    
    await application.initialize()
    await application.start()
    await application.updater.start_polling(drop_pending_updates=True)
    logger.info("Telegram bot started successfully with enhanced reliability.")
    
    yield
    
    await application.updater.stop()
    await application.stop()
    await application.shutdown()
    
    global global_session
    if global_session and not global_session.closed:
        await global_session.close()
        logger.info("Global session closed.")
    
    logger.info("Telegram bot has been shut down.")

web_app = FastAPI(lifespan=lifespan)

@web_app.api_route("/", methods=["GET", "HEAD"])
def health_check():
    return {"status": "ok, bot is running"}