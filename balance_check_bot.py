import asyncio
import logging
import os
import re
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Dict, List, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict

import aiohttp
from fastapi import FastAPI
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

# --- Standard Configuration ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Load Secrets from Environment Variables ---
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")

# --- Bot Configuration ---
TIP_ADDRESS = "0x50A0d7Fb9f64e908688443cB94c3971705599d79"
MAX_CONCURRENT_REQUESTS = 20  # Increased for better parallelism
REQUEST_TIMEOUT = 8  # Reduced timeout
MAX_RETRIES = 2  # Reduced retries
BATCH_SIZE = 10  # For batch RPC calls

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

async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    start_time = asyncio.get_event_loop().time()
    status_message = await update.message.reply_text("🔍 Resolving addresses...")
    
    try:
        # Use connection pooling for better performance
        connector = aiohttp.TCPConnector(
            limit=100, 
            ttl_dns_cache=300, 
            use_dns_cache=True,
            keepalive_timeout=30
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
            stablecoin_total = grand_totals.get('USDC', 0) + grand_totals.get('USDT', 0)
            if stablecoin_total > 0:
                portfolio_usd += stablecoin_total
                final_message += f"💵 **Stablecoin Value:** `${stablecoin_total:,.2f}`\n"
            
            if portfolio_usd > 0:
                final_message += f"🏦 **Total Portfolio Value:** `${portfolio_usd:,.2f}`\n"
        
        final_message += f"\n✅ Found balances on {total_chains_with_balances} chain(s)"
        
        await status_message.edit_text(final_message, parse_mode='Markdown', disable_web_page_preview=True)
        
        # Log summary for debugging
        logger.info(f"Completed balance check for {len(addresses)} addresses in {execution_time:.1f}s. "
                   f"Found balances on {total_chains_with_balances} chains. Grand totals: {grand_totals}")

    except Exception as e:
        logger.error(f"Error in balance_command: {e}", exc_info=True)
        await status_message.edit_text(f"❌ An error occurred while fetching balances. Please try again.\n\nError: {str(e)[:100]}")

# --- Lifespan Manager & Web Server Setup ---
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    if not TELEGRAM_TOKEN:
        logger.critical("CRITICAL: TELEGRAM_TOKEN not set.")
        yield
        return
        
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("about", about_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, balance_command))
    
    await application.initialize()
    await application.start()
    await application.updater.start_polling(drop_pending_updates=True)
    logger.info("Telegram bot started successfully.")
    
    yield
    
    await application.updater.stop()
    await application.stop()
    await application.shutdown()
    logger.info("Telegram bot has been shut down.")

web_app = FastAPI(lifespan=lifespan)

@web_app.api_route("/", methods=["GET", "HEAD"])
def health_check():
    return {"status": "ok, bot is running"}