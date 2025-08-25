import asyncio
import logging
import os
import re
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Dict, List, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict
import time

import aiohttp
from fastapi import FastAPI
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

# Rate limiting for users
user_last_request = {}
active_user_requests = defaultdict(int)

# --- Standard Configuration ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Load Secrets from Environment Variables ---
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")

# --- Bot Configuration ---
TIP_ADDRESS = "0x50A0d7Fb9f64e908688443cB94c3971705599d79"
MAX_CONCURRENT_REQUESTS = 15  # Reduced for stability under load
REQUEST_TIMEOUT = 6  # Faster timeout for quicker failover
MAX_RETRIES = 1  # Faster failure for better UX under load
BATCH_SIZE = 8  # Smaller batches for better resource management
USER_RATE_LIMIT = 3  # Max 3 concurrent requests per user

# Precomputed decimals to avoid extra RPC calls
TOKEN_DECIMALS = {
    'USDT': 6,
    'USDC': 6,
    # BSC-specific overrides for correct decimals
    'bsc_USDT': 18,  # BSC USDT uses 18 decimals, not 6!
    'bsc_USDC': 18,  # BSC USDC uses 18 decimals, not 6!
}

@dataclass
class BalanceCheck:
    chain_id: str
    address: str
    symbol: str
    contract: Optional[str] = None
    is_native: bool = True

# --- Enhanced Chain Configuration with Faster RPCs First ---
CHAINS = {
    'ethereum': {
        'name': 'Ethereum Mainnet', 
        'symbol': 'ETH', 
        'rpcs': [
            'https://eth.llamarpc.com',
            'https://rpc.ankr.com/eth',
            'https://ethereum.publicnode.com'
        ]
    },
    'base': {
        'name': 'Base', 
        'symbol': 'ETH', 
        'rpcs': [
            'https://mainnet.base.org',
            'https://base.publicnode.com',
            'https://base-mainnet.public.blastapi.io'
        ]
    },
    'arbitrum': {
        'name': 'Arbitrum', 
        'symbol': 'ETH', 
        'rpcs': [
            'https://arb1.arbitrum.io/rpc',
            'https://rpc.ankr.com/arbitrum',
            'https://arbitrum.publicnode.com'
        ]
    },
    'optimism': {
        'name': 'Optimism', 
        'symbol': 'ETH', 
        'rpcs': [
            'https://mainnet.optimism.io',
            'https://rpc.ankr.com/optimism',
            'https://optimism.publicnode.com'
        ]
    },
    'polygon': {
        'name': 'Polygon', 
        'symbol': 'MATIC', 
        'rpcs': [
            'https://polygon-rpc.com',
            'https://rpc.ankr.com/polygon',
            'https://polygon.publicnode.com'
        ]
    },
    'bsc': {
        'name': 'BSC', 
        'symbol': 'BNB', 
        'rpcs': [
            'https://bsc-dataseed.binance.org',
            'https://rpc.ankr.com/bsc',
            'https://bnb.publicnode.com'
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

# --- Optimized RPC and Balance Fetching ---

async def get_eth_price(session: aiohttp.ClientSession) -> float:
    """Fetch ETH price from CoinGecko"""
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {"ids": "ethereum", "vs_currencies": "usd"}
    if COINGECKO_API_KEY:
        params['x_cg_demo_api_key'] = COINGECKO_API_KEY
    
    try:
        async with session.get(url, params=params, timeout=5) as r:
            if r.status == 200:
                data = await r.json()
                return data.get('ethereum', {}).get('usd', 0.0)
    except Exception as e:
        logger.warning(f"ETH price fetch failed: {e}")
    return 0.0

async def make_batch_rpc_call(session: aiohttp.ClientSession, rpc_url: str, 
                            payloads: List[dict], context: str) -> List[Optional[dict]]:
    """Make batch RPC call for better efficiency"""
    if len(payloads) == 1:
        # Single call
        try:
            async with session.post(rpc_url, json=payloads[0], timeout=REQUEST_TIMEOUT) as response:
                if response.status == 200:
                    data = await response.json()
                    if 'error' not in data:
                        return [data]
                elif response.status == 429:
                    logger.warning(f"Rate limited on {rpc_url} for {context}")
                    return [None]
        except Exception as e:
            logger.warning(f"RPC call failed for {context}: {e}")
        return [None]
    
    # Batch call
    try:
        async with session.post(rpc_url, json=payloads, timeout=REQUEST_TIMEOUT) as response:
            if response.status == 200:
                data = await response.json()
                if isinstance(data, list):
                    return [item if 'error' not in item else None for item in data]
                else:
                    return [data if 'error' not in data else None]
            elif response.status == 429:
                logger.warning(f"Rate limited on {rpc_url} for batch {context}")
                return [None] * len(payloads)
    except Exception as e:
        logger.warning(f"Batch RPC call failed for {context}: {e}")
    
    return [None] * len(payloads)

async def process_balance_batch(session: aiohttp.ClientSession, 
                              checks: List[BalanceCheck], 
                              semaphore: asyncio.Semaphore) -> List[Tuple[str, str, float]]:
    """Process a batch of balance checks for the same chain"""
    if not checks:
        return []
    
    async with semaphore:
        chain_id = checks[0].chain_id
        rpc_urls = CHAINS[chain_id]['rpcs']
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
            
            for rpc_url in rpc_urls:
                native_responses = await make_batch_rpc_call(
                    session, rpc_url, native_payloads, f"native batch on {chain_id}"
                )
                
                success_count = 0
                for check, response in zip(native_checks, native_responses):
                    if response and 'result' in response:
                        try:
                            balance = int(response['result'], 16) / 10**18
                            results.append((check.chain_id, check.symbol, balance))
                            success_count += 1
                        except (ValueError, TypeError):
                            results.append((check.chain_id, check.symbol, 0.0))
                    else:
                        results.append((check.chain_id, check.symbol, 0.0))
                
                if success_count == len(native_checks):
                    break  # All succeeded, no need to try other RPCs
        
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
            
            for rpc_url in rpc_urls:
                erc20_responses = await make_batch_rpc_call(
                    session, rpc_url, erc20_payloads, f"ERC20 batch on {chain_id}"
                )
                
                success_count = 0
                for check, response in zip(erc20_checks, erc20_responses):
                    if response and 'result' in response:
                        try:
                            balance_raw = int(response['result'], 16)
                            # Fix BSC decimal issue - BSC stablecoins use 18 decimals
                            decimals_key = f"{check.chain_id}_{check.symbol}" if check.chain_id == 'bsc' else check.symbol
                            decimals = TOKEN_DECIMALS.get(decimals_key, 18)
                            balance = balance_raw / (10 ** decimals)
                            results.append((check.chain_id, check.symbol, balance))
                            success_count += 1
                        except (ValueError, TypeError):
                            results.append((check.chain_id, check.symbol, 0.0))
                    else:
                        results.append((check.chain_id, check.symbol, 0.0))
                
                if success_count == len(erc20_checks):
                    break  # All succeeded, no need to try other RPCs
        
        return results

async def get_all_asset_balances_optimized(session: aiohttp.ClientSession, addresses: List[str]) -> Dict:
    """Optimized balance fetching with batching and parallel processing"""
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
        # Split into smaller batches to avoid overwhelming single RPCs
        for i in range(0, len(checks), BATCH_SIZE):
            batch = checks[i:i + BATCH_SIZE]
            tasks.append(process_balance_batch(session, batch, semaphore))
    
    logger.info(f"Processing {len(tasks)} batches across {len(chain_groups)} chains")
    
    # Execute all batches concurrently
    batch_results = await asyncio.gather(*tasks, return_exceptions=True)
    
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
    
    logger.info(f"Found {total_balances} non-zero balances across {len(aggregated)} chains")
    return aggregated

# --- Address Parsing and ENS Resolution ---

async def resolve_ens_to_address(session: aiohttp.ClientSession, name: str) -> str | None:
    """Resolve ENS name to address with timeout"""
    try:
        async with session.get(f"https://api.ensideas.com/ens/resolve/{name.lower()}", 
                             timeout=5) as response:
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
    # Enhanced patterns
    address_pattern = r'0x[a-fA-F0-9]{40}'
    ens_pattern = r'[a-zA-Z0-9-]+\.eth'
    
    # Also look for addresses in common formats like "Address: 0x..." or "Wallet: 0x..."
    context_address_pattern = r'(?:address|wallet|addr|account)[:=\s]+0x[a-fA-F0-9]{40}'
    
    found_addresses = set()
    found_ens_names = set()
    
    # Standard extraction
    found_addresses.update(addr.lower() for addr in re.findall(address_pattern, text, re.IGNORECASE))
    found_ens_names.update(name.lower() for name in re.findall(ens_pattern, text, re.IGNORECASE))
    
    # Extract from contextual patterns
    contextual_matches = re.findall(context_address_pattern, text, re.IGNORECASE)
    for match in contextual_matches:
        addr_match = re.search(r'0x[a-fA-F0-9]{40}', match, re.IGNORECASE)
        if addr_match:
            found_addresses.add(addr_match.group().lower())
    
    logger.info(f"Found {len(found_addresses)} addresses and {len(found_ens_names)} ENS names")
    
    # Resolve ENS names concurrently
    if found_ens_names:
        resolved_addresses = await asyncio.gather(
            *(resolve_ens_to_address(session, name) for name in found_ens_names),
            return_exceptions=True
        )
        
        for addr in resolved_addresses:
            if isinstance(addr, str) and addr:
                found_addresses.add(addr.lower())
    
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

⚡ **Now with lightning-fast batch processing!**
"""
    await update.message.reply_text(welcome_message, parse_mode='Markdown', disable_web_page_preview=False)

async def about_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    about_message = f"Tip Jar (ETH/EVM):\n`{TIP_ADDRESS}`"
    await update.message.reply_text(about_message, parse_mode='Markdown')

async def debug_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Test RPC endpoint speeds for debugging"""
    # You can restrict this to your user ID for security
    # user_id = update.effective_user.id
    # if user_id != YOUR_TELEGRAM_USER_ID:  # Replace with your actual user ID
    #     await update.message.reply_text("❌ Debug command is admin-only.")
    #     return
    
    status_message = await update.message.reply_text("🔧 Testing RPC endpoint speeds...")
    
    test_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"  # Vitalik's address
    results = []
    
    connector = aiohttp.TCPConnector(limit=20)
    async with aiohttp.ClientSession(connector=connector) as session:
        
        for chain_id, chain_info in CHAINS.items():
            chain_results = []
            
            for rpc_url in chain_info['rpcs'][:2]:  # Test first 2 RPCs per chain
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
                chain_results.append(f"  {rpc_name}: {status}")
            
            results.append(f"**{chain_info['name']}:**\n" + "\n".join(chain_results))
    
    debug_message = "🔧 **RPC Endpoint Speed Test:**\n\n" + "\n\n".join(results)
    debug_message += f"\n\n💡 **Analysis:** Look for endpoints consistently >3s or with errors."
    
    await status_message.edit_text(debug_message, parse_mode='Markdown')

async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    current_time = time.time()
    
    # Rate limiting: max 1 request per 10 seconds per user
    if user_id in user_last_request:
        time_since_last = current_time - user_last_request[user_id]
        if time_since_last < 10:
            await update.message.reply_text(f"⏳ Please wait {10 - int(time_since_last)} seconds before your next request.")
            return
    
    # Check concurrent requests for this user
    if active_user_requests[user_id] >= USER_RATE_LIMIT:
        await update.message.reply_text("⚠️ You have too many active requests. Please wait for them to complete.")
        return
    
    user_last_request[user_id] = current_time
    active_user_requests[user_id] += 1
    
    try:
        start_time = asyncio.get_event_loop().time()
        status_message = await update.message.reply_text("🔍 Resolving addresses...")
        
        try:
            # Use smaller connection pool under load
            connector = aiohttp.TCPConnector(
                limit=50,  # Reduced from 100
                ttl_dns_cache=300, 
                use_dns_cache=True,
                keepalive_timeout=15  # Shorter keepalive
            )
            
            async with aiohttp.ClientSession(connector=connector) as session:
                # Parse and resolve addresses
                addresses = await parse_and_resolve_addresses(session, update.message.text)
                if not addresses:
                    await status_message.edit_text("❌ I didn't find any valid addresses or `.eth` names.")
                    return

                if len(addresses) > 200:
                    await status_message.edit_text("❌ Too many addresses! Please limit to 200 addresses maximum.")
                    return

                await status_message.edit_text(f"✅ Found {len(addresses)} unique address(es).\n⚡ Fetching all balances with optimized batch processing...")
                
                # Get balances and ETH price concurrently
                balance_task = get_all_asset_balances_optimized(session, addresses)
                price_task = get_eth_price(session)
                
                aggregated_balances, eth_price = await asyncio.gather(balance_task, price_task)
    
        if not aggregated_balances:
            await status_message.edit_text("❌ No balances found for the provided addresses.")
            return

        # Calculate totals
        grand_totals = {}
        total_chains_with_balances = len(aggregated_balances)
        
        for chain_id, tokens in aggregated_balances.items():
            for symbol, balance in tokens.items():
                if symbol not in grand_totals:
                    grand_totals[symbol] = 0
                grand_totals[symbol] += balance

        # Build response message with better formatting
        execution_time = asyncio.get_event_loop().time() - start_time
        final_message = f"📊 **Balance Summary for {len(addresses)} address(es)** (⚡ {execution_time:.1f}s)\n\n"
        
        # Sort chains by total USD value if possible, otherwise alphabetically
        chain_items = list(aggregated_balances.items())
        try:
            # Sort by ETH value first (proxy for importance), then alphabetically
            chain_items.sort(key=lambda x: (
                -x[1].get('ETH', 0),  # Descending ETH amount
                x[0]  # Ascending chain name
            ))
        except:
            chain_items.sort()  # Fallback to alphabetical
        
        # Per-chain breakdown with better formatting
        for chain_id, tokens in chain_items:
            chain_name = CHAINS[chain_id]['name']
            token_lines = []
            
            # Sort tokens by value (ETH first, then stablecoins, then alphabetical)
            token_items = list(tokens.items())
            token_items.sort(key=lambda x: (
                0 if x[0] in ['ETH'] else 1 if x[0] in ['USDC', 'USDT'] else 2,  # Priority order
                -x[1]  # Descending balance within category
            ))
            
            for symbol, balance in token_items:
                if balance >= 0.000001:  # Only show meaningful balances
                    if symbol in ['USDC', 'USDT']:
                        # Format stablecoins with 2 decimal places for readability
                        formatted_balance = f"{balance:,.2f}"
                    elif symbol in ['ETH', 'BNB', 'MATIC']:
                        # Format native tokens with up to 6 decimals, removing trailing zeros
                        formatted_balance = f"{balance:,.6f}".rstrip('0').rstrip('.')
                    else:
                        formatted_balance = f"{balance:,.6f}".rstrip('0').rstrip('.')
                    
                    token_lines.append(f"{formatted_balance} {symbol}")
            
            if token_lines:
                final_message += f"• **{chain_name}:** {', '.join(token_lines)}\n"
        
        # Grand totals with better formatting
        if grand_totals:
            final_message += "\n" + "="*30 + "\n**🎯 GRAND TOTALS ACROSS ALL CHAINS:**\n"
            
            # Sort totals by importance
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

            # Calculate total portfolio value if possible
            portfolio_usd = 0
            if 'ETH' in grand_totals and grand_totals['ETH'] > 0 and eth_price > 0:
                eth_usd = grand_totals['ETH'] * eth_price
                portfolio_usd += eth_usd
                final_message += f"\n💰 **ETH Value:** `${eth_usd:,.2f}` (@ `${eth_price:,.2f}/ETH`)\n"
            
            # Add stablecoin values