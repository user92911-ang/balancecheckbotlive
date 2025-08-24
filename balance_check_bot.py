import asyncio
import logging
import os
import re
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Dict, List

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

# --- Chain & Token Configuration ---
CHAINS = {
    'ethereum': {'name': 'Ethereum Mainnet', 'symbol': 'ETH', 'rpc': 'https://eth.llamarpc.com'},
    'base': {'name': 'Base', 'symbol': 'ETH', 'rpc': 'https://mainnet.base.org'},
    'arbitrum': {'name': 'Arbitrum', 'symbol': 'ETH', 'rpc': 'https://arb1.arbitrum.io/rpc'},
    'optimism': {'name': 'Optimism', 'symbol': 'ETH', 'rpc': 'https://mainnet.optimism.io'},
    'polygon': {'name': 'Polygon', 'symbol': 'MATIC', 'rpc': 'https://polygon-rpc.com'},
    'bsc': {'name': 'BSC', 'symbol': 'BNB', 'rpc': 'https://bsc-dataseed.binance.org'},
    'ink': {'name': 'Ink', 'symbol': 'ETH', 'rpc': 'https://rpc-gel.inkonchain.com'},
    'hyperliquid': {'name': 'Hyperliquid', 'symbol': 'ETH', 'rpc': 'https://rpc.hyperliquid.xyz/evm'},
    'unichain': {'name': 'Unichain', 'symbol': 'ETH', 'rpc': 'https://mainnet.unichain.org'},
    'abstract': {'name': 'Abstract', 'symbol': 'ETH', 'rpc': 'https://api.mainnet.abs.xyz'},
}

ERC20_CONTRACTS = {
    'ethereum': {
        'USDT': '0xdac17f958d2ee523a2206206994597c13d831ec7',
        'USDC': '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'
    },
    'base': {
        'USDT': '0xfde4C96c8593536E31F229EA8f37b2ADa2699bb2',
        'USDC': '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'
    },
    'ink': {
        'USDT': '0x0200C29006150606B650577BBE7B6248F58470c1'
    },
    'arbitrum': {
        'USDC': '0xaf88d065e77c8cC2239327C5EDb3A432268e5831',
        'USDT': '0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9'
    },
    'hyperliquid': {
        'HYPE': '0x0d01dc56dcaaca66ad901c959b4011ec'
    },
    'unichain': {
        'USDC': '0x0d01dc56dcaaca66ad901c959b4011ec',
        'USDT': '0x9151434b16b9763660705744891fA906F660EcC5'
    },
    'polygon': {
        'USDC': '0x3c499c542cef5e3811e1192ce70d8cc03d5c3359',
        'USDT': '0xc2132D05D31c914a87C6611C10748AEb04B58e8F'
    },
    'bsc': {
        'USDT': '0x55d398326f99059ff775485246999027b3197955'
    },
    'abstract': {
        'USDT': '0x0709F39376dEEe2A2dfC94A58EdEb2Eb9DF012bD',
        'USDC': '0x84A71ccD554Cc1b02749b35d22F684CC8ec987e1'
    }
}


# --- Balance Fetching Logic ---

async def get_eth_price(session: aiohttp.ClientSession) -> float:
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {"ids": "ethereum", "vs_currencies": "usd"}
    if COINGECKO_API_KEY: params['x_cg_demo_api_key'] = COINGECKO_API_KEY
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        async with session.get(url, params=params, headers=headers, timeout=10) as response:
            if response.status == 200:
                data = await response.json()
                if 'ethereum' in data and 'usd' in data['ethereum']: return data['ethereum']['usd']
    except Exception as e: logger.error(f"Could not fetch ETH price: {e}")
    return 0.0

async def get_native_balance(session: aiohttp.ClientSession, rpc_url: str, address: str) -> float:
    try:
        payload = {"jsonrpc": "2.0", "method": "eth_getBalance", "params": [address, "latest"], "id": 1}
        async with session.post(rpc_url, json=payload, timeout=10) as response:
            if response.status == 200:
                data = await response.json()
                if 'result' in data: return int(data['result'], 16) / 10**18
    except Exception: pass
    return 0.0

async def get_erc20_balance(session: aiohttp.ClientSession, rpc_url: str, contract: str, address: str) -> float:
    balance_payload = {"jsonrpc": "2.0", "method": "eth_call", "params": [{"to": contract, "data": f"0x70a08231{address[2:].zfill(64)}"}, "latest"], "id": 1}
    decimals_payload = {"jsonrpc": "2.0", "method": "eth_call", "params": [{"to": contract, "data": "0x313ce567"}, "latest"], "id": 1}
    try:
        async with session.post(rpc_url, json=balance_payload) as b_resp, session.post(rpc_url, json=decimals_payload) as d_resp:
            if b_resp.status == 200 and d_resp.status == 200:
                balance_data, decimals_data = await b_resp.json(), await d_resp.json()
                if balance_data.get('result') not in (None, '0x') and decimals_data.get('result') not in (None, '0x'):
                    return int(balance_data['result'], 16) / (10**int(decimals_data['result'], 16))
    except Exception: pass
    return 0.0

async def get_all_asset_balances(session: aiohttp.ClientSession, addresses: List[str]) -> Dict:
    all_balances = {addr: {} for addr in addresses}
    async def fetch_for_address(addr):
        tasks = []
        for chain_id, chain_info in CHAINS.items():
            tasks.append(fetch_native(session, chain_info['rpc'], addr, chain_id, chain_info['symbol']))
            if chain_id in ERC20_CONTRACTS:
                for symbol, contract in ERC20_CONTRACTS[chain_id].items():
                    tasks.append(fetch_erc20(session, chain_info['rpc'], contract, addr, chain_id, symbol))
        results = await asyncio.gather(*tasks)
        for chain_id, symbol, balance in results:
            if balance and balance > 0.000001:
                if chain_id not in all_balances[addr]: all_balances[addr][chain_id] = {}
                all_balances[addr][chain_id][symbol] = balance
    async def fetch_native(session, rpc, addr, chain_id, symbol): return chain_id, symbol, await get_native_balance(session, rpc, addr)
    async def fetch_erc20(session, rpc, contract, addr, chain_id, symbol): return chain_id, symbol, await get_erc20_balance(session, rpc, contract, addr)
    await asyncio.gather(*(fetch_for_address(addr) for addr in addresses))
    return all_balances

# --- Address Parsing and ENS Resolution ---

async def resolve_ens_to_address(session: aiohttp.ClientSession, name: str) -> str | None:
    # Using a reliable public API for ENS resolution is simpler than a full on-chain implementation.
    try:
        async with session.get(f"https://api.ensideas.com/ens/resolve/{name.lower()}") as response:
            if response.status == 200:
                data = await response.json()
                return data.get("address")
    except Exception: return None

async def parse_and_resolve_addresses(session: aiohttp.ClientSession, text: str) -> List[str]:
    address_pattern = r'0x[a-fA-F0-9]{40}'
    ens_pattern = r'[a-zA-Z0-9-]+\.eth'
    found_addresses = {addr.lower() for addr in re.findall(address_pattern, text)}
    found_ens_names = {name.lower() for name in re.findall(ens_pattern, text)}
    resolved_addresses = await asyncio.gather(*(resolve_ens_to_address(session, name) for name in found_ens_names))
    for addr in resolved_addresses:
        if addr: found_addresses.add(addr.lower())
    return list(found_addresses)

# --- Command Handlers ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # IMPORTANT: Replace the URL below with a direct link to your image.
    image_url = "https://i.ibb.co/qMZjmJ9V/eth2.png"
    welcome_message = f"[​]({image_url})" # Zero-width space trick for image preview
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
    about_message = f"""
This bot was created to easily track balances across multiple wallets and chains.
If you find it useful, please consider supporting its hosting costs. Tips are greatly appreciated!

**Tip Jar (ETH/EVM):**
`{TIP_ADDRESS}`
    """
    await update.message.reply_text(about_message, parse_mode='Markdown')

async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    status_message = await update.message.reply_text("🔍 Resolving addresses...")
    async with aiohttp.ClientSession() as session:
        addresses = await parse_and_resolve_addresses(session, update.message.text)
        if not addresses:
            await status_message.edit_text("I didn't find any valid wallet addresses or `.eth` names to check. Use /start for instructions."); return

        await status_message.edit_text(f"✅ Found {len(addresses)} unique address(es). Fetching all balances now, this may take a moment...")
        all_balances = await get_all_asset_balances(session, addresses)
        eth_price = await get_eth_price(session)
    
    grand_totals = {}
    final_message = "📊 **Balance Summary**\n"
    for addr in addresses:
        if all_balances.get(addr):
            short_addr = f"{addr[:6]}...{addr[-4:]}"
            final_message += f"\n**Wallet: `{short_addr}`**\n"
            for chain_id, tokens in sorted(all_balances[addr].items()):
                chain_name = CHAINS[chain_id]['name']
                token_lines = []
                for symbol, balance in sorted(tokens.items()):
                    token_lines.append(f"{balance:,.4f} {symbol}")
                    if symbol not in grand_totals: grand_totals[symbol] = 0
                    grand_totals[symbol] += balance
                final_message += f" • **{chain_name}:** {', '.join(token_lines)}\n"
    
    if not grand_totals:
        await status_message.edit_text("No balances found for the provided addresses on any supported chain."); return

    final_message += "\n---\n**GRAND TOTALS ACROSS ALL WALLETS**\n"
    for symbol, total in sorted(grand_totals.items()):
        final_message += f"🎯 **{symbol}:** {total:,.4f}\n"
    if 'ETH' in grand_totals and grand_totals['ETH'] > 0 and eth_price > 0:
        usd_value = grand_totals['ETH'] * eth_price
        final_message += f"💰 **Total ETH Value:** `${usd_value:,.2f}` (@ `${eth_price:,.2f}/ETH`)"
    
    await status_message.edit_text(final_message, parse_mode='Markdown', disable_web_page_preview=True)

# --- Lifespan Manager & Web Server Setup ---
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    if not TELEGRAM_TOKEN:
        logger.critical("CRITICAL: TELEGRAM_TOKEN environment variable not set. Bot will not start."); yield; return
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("about", about_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, balance_command))
    await application.initialize()
    await application.start()
    await application.updater.start_polling(drop_pending_updates=True)
    logger.info("Telegram bot has started successfully."); yield
    await application.updater.stop(); await application.stop(); await application.shutdown()
    logger.info("Telegram bot has been shut down.")

web_app = FastAPI(lifespan=lifespan)
@web_app.api_route("/", methods=["GET", "HEAD"])
def health_check(): return {"status": "ok, bot is running"}