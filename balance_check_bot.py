import asyncio
import logging
import os
import re
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Dict, List, Optional, Tuple

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
MAX_CONCURRENT_REQUESTS = 8  # Further reduced for stability
REQUEST_TIMEOUT = 15  # Balanced timeout
MAX_RETRIES = 2  # Fewer retries for speed
RETRY_DELAY = 0.5  # Faster retry delay
REQUEST_DELAY = 0.1  # Delay between requests to avoid rate limits

# --- Chain & Token Configuration with RPC Fallbacks ---
CHAINS = {
    'ethereum': {'name': 'Ethereum Mainnet', 'symbol': 'ETH', 'rpcs': ['https://eth.llamarpc.com', 'https://api.stateless.solutions/ethereum/v1/demo']},
    'base': {'name': 'Base', 'symbol': 'ETH', 'rpcs': ['https://mainnet.base.org', 'https://base.publicnode.com']},
    'arbitrum': {'name': 'Arbitrum', 'symbol': 'ETH', 'rpcs': ['https://arb1.arbitrum.io/rpc', 'https://arbitrum.publicnode.com']},
    'optimism': {'name': 'Optimism', 'symbol': 'ETH', 'rpcs': ['https://mainnet.optimism.io', 'https://optimism.publicnode.com']},
    'polygon': {'name': 'Polygon', 'symbol': 'MATIC', 'rpcs': ['https://polygon-rpc.com', 'https://polygon.publicnode.com']},
    'bsc': {'name': 'BSC', 'symbol': 'BNB', 'rpcs': ['https://bsc-dataseed.binance.org', 'https://bnb.publicnode.com']},
    'ink': {'name': 'Ink', 'symbol': 'ETH', 'rpcs': ['https://rpc-gel.inkonchain.com']},
    'hyperliquid': {'name': 'Hyperliquid', 'symbol': 'ETH', 'rpcs': ['https://rpc.hyperliquid.xyz/evm']},
    'unichain': {'name': 'Unichain', 'symbol': 'ETH', 'rpcs': ['https://mainnet.unichain.org']},
    'abstract': {'name': 'Abstract', 'symbol': 'ETH', 'rpcs': ['https://api.mainnet.abs.xyz']},
}

ERC20_CONTRACTS = {
    'ethereum': {'USDT': '0xdac17f958d2ee523a2206206994597c13d831ec7', 'USDC': '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'},
    'base': {'USDT': '0xfde4C96c8593536E31F229EA8f37b2ADa2699bb2', 'USDC': '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'},
    'ink': {'USDT': '0x0200C29006150606B650577BBE7B6248F58470c1'},
    'arbitrum': {'USDC': '0xaf88d065e77c8cC2239327C5EDb3A432268e5831', 'USDT': '0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9'},
    # Removed problematic contracts with invalid addresses
    # 'hyperliquid': {'HYPE': '0x0d01dc56dcaaca66ad901c959b4011ec'},  # Invalid address length
    # 'unichain': {'USDC': '0x0d01dc56dcaaca66ad901c959b4011ec', 'USDT': '0x9151434b16b9763660705744891fA906F660EcC5'},  # Invalid USDC address
    'unichain': {'USDT': '0x9151434b16b9763660705744891fA906F660EcC5'},  # Only valid USDT address
    'polygon': {'USDC': '0x3c499c542cef5e3811e1192ce70d8cc03d5c3359', 'USDT': '0xc2132D05D31c914a87C6611C10748AEb04B58e8F'},
    'bsc': {'USDT': '0x55d398326f99059ff775485246999027b3197955'},
    'abstract': {'USDT': '0x0709F39376dEEe2A2dfC94A58EdEb2Eb9DF012bD', 'USDC': '0x84A71ccD554Cc1b02749b35d22F684CC8ec987e1'}
}

class BalanceFetchError(Exception):
    """Custom exception for balance fetching errors"""
    pass

# --- Enhanced Balance Fetching Logic with Better Error Handling ---

async def get_eth_price(session: aiohttp.ClientSession) -> float:
    """Fetch ETH price from CoinGecko with better error handling"""
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {"ids": "ethereum", "vs_currencies": "usd"}
    if COINGECKO_API_KEY:
        params['x_cg_demo_api_key'] = COINGECKO_API_KEY
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT) as r:
                if r.status == 200:
                    data = await r.json()
                    if 'ethereum' in data and 'usd' in data['ethereum']:
                        price = data['ethereum']['usd']
                        logger.info(f"Fetched ETH price: ${price}")
                        return price
                else:
                    logger.warning(f"CoinGecko API returned status {r.status}")
        except Exception as e:
            logger.warning(f"ETH price fetch attempt {attempt + 1} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY)
    
    logger.error("Failed to fetch ETH price after all attempts")
    return 0.0

async def make_rpc_call_with_retries(session: aiohttp.ClientSession, rpc_url: str, payload: dict, context: str) -> Optional[dict]:
    """Make RPC call with retries, rate limiting awareness, and better error handling"""
    for attempt in range(MAX_RETRIES):
        try:
            # Add small delay to avoid overwhelming RPCs
            if attempt > 0:
                await asyncio.sleep(REQUEST_DELAY * attempt)
                
            async with session.post(rpc_url, json=payload, timeout=REQUEST_TIMEOUT) as response:
                if response.status == 200:
                    data = await response.json()
                    if 'error' in data:
                        logger.warning(f"RPC error for {context}: {data['error'].get('message', 'Unknown error')}")
                        return None  # Don't retry on RPC errors
                    return data
                elif response.status == 429:  # Rate limited
                    wait_time = 2 ** attempt  # Exponential backoff for rate limits
                    logger.warning(f"Rate limited for {context} on {rpc_url}, waiting {wait_time}s")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.warning(f"HTTP {response.status} for {context} on {rpc_url}")
        except asyncio.TimeoutError:
            logger.warning(f"Timeout for {context} on {rpc_url} (attempt {attempt + 1})")
        except Exception as e:
            logger.warning(f"Error for {context} on {rpc_url} (attempt {attempt + 1}): {e}")
        
        if attempt < MAX_RETRIES - 1:
            await asyncio.sleep(RETRY_DELAY * (attempt + 1))
    
    return None

async def get_native_balance_with_fallback(session: aiohttp.ClientSession, rpc_urls: List[str], 
                                         address: str, chain_id: str, semaphore: asyncio.Semaphore) -> float:
    """Get native balance with comprehensive fallback and error tracking"""
    async with semaphore:
        context = f"native balance for {address[:6]}...{address[-4:]} on {chain_id}"
        
        for rpc_url in rpc_urls:
            payload = {
                "jsonrpc": "2.0",
                "method": "eth_getBalance",
                "params": [address, "latest"],
                "id": 1
            }
            
            data = await make_rpc_call_with_retries(session, rpc_url, payload, context)
            if data and 'result' in data and data['result']:
                try:
                    balance = int(data['result'], 16) / 10**18
                    if balance > 0:
                        logger.info(f"Found native balance: {balance:.6f} on {chain_id} for {address[:6]}...")
                    return balance
                except (ValueError, TypeError) as e:
                    logger.warning(f"Failed to parse native balance result for {context}: {e}")
                    continue
        
        logger.error(f"Failed to get {context} from all RPCs: {rpc_urls}")
        return 0.0

async def get_erc20_balance_with_fallback(session: aiohttp.ClientSession, rpc_urls: List[str], 
                                        contract: str, address: str, chain_id: str, symbol: str,
                                        semaphore: asyncio.Semaphore) -> float:
    """Get ERC20 balance with comprehensive fallback and error tracking"""
    async with semaphore:
        context = f"{symbol} balance for {address[:6]}...{address[-4:]} on {chain_id}"
        
        for rpc_url in rpc_urls:
            try:
                # Prepare payloads
                balance_payload = {
                    "jsonrpc": "2.0",
                    "method": "eth_call",
                    "params": [{
                        "to": contract,
                        "data": f"0x70a08231{address[2:].zfill(64)}"
                    }, "latest"],
                    "id": 1
                }
                
                decimals_payload = {
                    "jsonrpc": "2.0",
                    "method": "eth_call",
                    "params": [{
                        "to": contract,
                        "data": "0x313ce567"
                    }, "latest"],
                    "id": 2
                }
                
                # Make both calls
                balance_data = await make_rpc_call_with_retries(session, rpc_url, balance_payload, f"{context} (balance)")
                decimals_data = await make_rpc_call_with_retries(session, rpc_url, decimals_payload, f"{context} (decimals)")
                
                if (balance_data and 'result' in balance_data and balance_data['result'] and 
                    decimals_data and 'result' in decimals_data and decimals_data['result']):
                    
                    try:
                        balance_raw = int(balance_data['result'], 16)
                        decimals = int(decimals_data['result'], 16)
                        
                        if balance_raw > 0:  # Only calculate if there's a balance
                            balance = balance_raw / (10 ** decimals)
                            logger.info(f"Found {symbol} balance: {balance:.6f} on {chain_id} for {address[:6]}...")
                            return balance
                        return 0.0
                        
                    except (ValueError, TypeError, OverflowError) as e:
                        logger.warning(f"Failed to parse {context} result: {e}")
                        continue
            except Exception as e:
                logger.warning(f"Unexpected error for {context} on {rpc_url}: {e}")
                continue
        
        logger.error(f"Failed to get {context} from all RPCs: {rpc_urls}")
        return 0.0

async def get_all_asset_balances(session: aiohttp.ClientSession, addresses: List[str]) -> Dict:
    """Get all asset balances with improved task management and prioritization"""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    logger.info(f"Starting balance checks for {len(addresses)} addresses across {len(CHAINS)} chains")
    
    # Prioritize chains by reliability (most reliable first)
    chain_priority = ['ethereum', 'arbitrum', 'polygon', 'bsc', 'optimism', 'base', 'ink', 'abstract', 'unichain', 'hyperliquid']
    
    # Group tasks by priority
    high_priority_tasks = []  # Native balances (faster)
    low_priority_tasks = []   # ERC20 balances (slower)
    
    for addr in addresses:
        for chain_id in chain_priority:
            if chain_id not in CHAINS:
                continue
                
            chain_info = CHAINS[chain_id]
            
            # Native balance task (high priority)
            high_priority_tasks.append(fetch_native_balance(session, chain_info['rpcs'], addr, chain_id, 
                                                          chain_info['symbol'], semaphore))
            
            # ERC20 balance tasks (low priority)
            if chain_id in ERC20_CONTRACTS:
                for symbol, contract in ERC20_CONTRACTS[chain_id].items():
                    # Validate contract address
                    if len(contract) != 42 or not contract.startswith('0x'):
                        logger.warning(f"Invalid contract address for {symbol} on {chain_id}: {contract}")
                        continue
                    low_priority_tasks.append(fetch_erc20_balance(session, chain_info['rpcs'], contract, addr, 
                                                                chain_id, symbol, semaphore))
    
    logger.info(f"Created {len(high_priority_tasks)} native and {len(low_priority_tasks)} ERC20 balance tasks")
    
    # Execute high priority tasks first (native balances)
    results = []
    
    # Process native balances in small batches
    batch_size = 20
    for i in range(0, len(high_priority_tasks), batch_size):
        batch = high_priority_tasks[i:i + batch_size]
        batch_results = await asyncio.gather(*batch, return_exceptions=True)
        
        for result in batch_results:
            if isinstance(result, Exception):
                logger.error(f"Native balance task failed: {result}")
            else:
                results.append(result)
        
        # Small delay between batches to be nice to RPCs
        await asyncio.sleep(0.1)
    
    logger.info(f"Completed {len(high_priority_tasks)} native balance checks")
    
    # Process ERC20 balances in smaller batches with delays
    batch_size = 15
    for i in range(0, len(low_priority_tasks), batch_size):
        batch = low_priority_tasks[i:i + batch_size]
        batch_results = await asyncio.gather(*batch, return_exceptions=True)
        
        for result in batch_results:
            if isinstance(result, Exception):
                logger.error(f"ERC20 balance task failed: {result}")
            else:
                results.append(result)
        
        # Longer delay between ERC20 batches
        await asyncio.sleep(0.2)
        
        if (i // batch_size + 1) % 5 == 0:  # Progress update every 5 batches
            logger.info(f"Completed {min(i + batch_size, len(low_priority_tasks))}/{len(low_priority_tasks)} ERC20 balance checks")
    
    logger.info(f"Completed all {len(low_priority_tasks)} ERC20 balance checks")
    
    # Aggregate results
    aggregated = {}
    successful_checks = 0
    
    for chain_id, symbol, balance in results:
        if balance and balance > 0.000001:  # Filter out dust
            if chain_id not in aggregated:
                aggregated[chain_id] = {}
            if symbol not in aggregated[chain_id]:
                aggregated[chain_id][symbol] = 0
            aggregated[chain_id][symbol] += balance
            successful_checks += 1
    
    logger.info(f"Aggregated {successful_checks} non-zero balances across {len(aggregated)} chains")
    return aggregated

async def fetch_native_balance(session, rpcs, addr, chain_id, symbol, sem) -> Tuple[str, str, float]:
    """Wrapper function for native balance fetching"""
    balance = await get_native_balance_with_fallback(session, rpcs, addr, chain_id, sem)
    return chain_id, symbol, balance

async def fetch_erc20_balance(session, rpcs, contract, addr, chain_id, symbol, sem) -> Tuple[str, str, float]:
    """Wrapper function for ERC20 balance fetching"""
    balance = await get_erc20_balance_with_fallback(session, rpcs, contract, addr, chain_id, symbol, sem)
    return chain_id, symbol, balance

# --- Address Parsing and ENS Resolution ---

async def resolve_ens_to_address(session: aiohttp.ClientSession, name: str) -> str | None:
    """Resolve ENS name to address with better error handling"""
    try:
        async with session.get(f"https://api.ensideas.com/ens/resolve/{name.lower()}", 
                             timeout=10) as response:
            if response.status == 200:
                data = await response.json()
                address = data.get("address")
                if address:
                    logger.info(f"Resolved ENS {name} to {address}")
                return address
            else:
                logger.warning(f"ENS resolution failed for {name}: HTTP {response.status}")
    except Exception as e:
        logger.warning(f"ENS resolution error for {name}: {e}")
    return None

async def parse_and_resolve_addresses(session: aiohttp.ClientSession, text: str) -> List[str]:
    """Parse and resolve addresses with improved validation"""
    address_pattern = r'0x[a-fA-F0-9]{40}'
    ens_pattern = r'[a-zA-Z0-9-]+\.eth'
    
    found_addresses = {addr.lower() for addr in re.findall(address_pattern, text)}
    found_ens_names = {name.lower() for name in re.findall(ens_pattern, text)}
    
    logger.info(f"Found {len(found_addresses)} addresses and {len(found_ens_names)} ENS names")
    
    # Resolve ENS names
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
    🤖 **Crypto Balance Bot**

I'll help you check native and stablecoin balances across multiple EVM chains! I also resolve `.eth` names.

**Commands:**
/start - Show this help message
/about - Info & support the creator

**Supported Chains:**
• Ethereum Mainnet
• Base
• Ink
• Arbitrum
• Abstract
• Hyperliquid
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
"""
    await update.message.reply_text(welcome_message, parse_mode='Markdown', disable_web_page_preview=False)

async def about_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    about_message = f"Tip Jar (ETH/EVM):\n`{TIP_ADDRESS}`"
    await update.message.reply_text(about_message, parse_mode='Markdown')

async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    start_time = asyncio.get_event_loop().time()
    status_message = await update.message.reply_text("🔍 Resolving addresses...")
    
    try:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=100, ttl_dns_cache=300)) as session:
            # Parse and resolve addresses
            addresses = await parse_and_resolve_addresses(session, update.message.text)
            if not addresses:
                await status_message.edit_text("❌ I didn't find any valid addresses or `.eth` names.")
                return

            if len(addresses) > 200:
                await status_message.edit_text("❌ Too many addresses! Please limit to 200 addresses maximum.")
                return

            await status_message.edit_text(f"✅ Found {len(addresses)} unique address(es).\n🔄 Fetching all balances across all chains...")
            
            # Get balances and ETH price concurrently
            balance_task = get_all_asset_balances(session, addresses)
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

        # Build response message
        execution_time = asyncio.get_event_loop().time() - start_time
        final_message = f"📊 **Balance Summary for {len(addresses)} address(es)** (⏱️ {execution_time:.1f}s)\n\n"
        
        # Per-chain breakdown
        for chain_id, tokens in sorted(aggregated_balances.items()):
            chain_name = CHAINS[chain_id]['name']
            token_lines = []
            for symbol, balance in sorted(tokens.items()):
                if balance >= 0.000001:  # Only show meaningful balances
                    token_lines.append(f"{balance:,.6f} {symbol}".rstrip('0').rstrip('.'))
            
            if token_lines:
                final_message += f"• **{chain_name}:** {', '.join(token_lines)}\n"
        
        # Grand totals
        if grand_totals:
            final_message += "\n" + "="*30 + "\n**🎯 GRAND TOTALS ACROSS ALL CHAINS:**\n"
            for symbol, total in sorted(grand_totals.items()):
                if total >= 0.000001:
                    formatted_total = f"{total:,.6f}".rstrip('0').rstrip('.')
                    final_message += f"**{symbol}:** {formatted_total}\n"

            # ETH USD value
            if 'ETH' in grand_totals and grand_totals['ETH'] > 0 and eth_price > 0:
                usd_value = grand_totals['ETH'] * eth_price
                final_message += f"\n💰 **Total ETH Value:** `${usd_value:,.2f}` (@ `${eth_price:,.2f}/ETH`)\n"
        
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