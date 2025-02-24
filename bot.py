import nest_asyncio
nest_asyncio.apply()

import asyncio
from datetime import datetime
import pytz
import ccxt.async_support as ccxt
from ccxt.base.errors import RateLimitExceeded
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
import certifi
import ssl

# ================================
# Настройки бота и арбитража
# ================================
TELEGRAM_TOKEN = "8152299637:AAHtKXAftrg-0Q6MRxWEYubbb_3G6EO2ThU"  # Ваш токен
THRESHOLD = 0.01  # Минимальный порог спреда (1%)
MAX_SPREAD = 0.70  # Если спред > 50%, сигнал не отправляется

registered_users = set()

# Создаем ssl контекст для продакшена
ssl_context = ssl.create_default_context(cafile=certifi.where())

# ================================
# Словарь комиссий (taker fee) для бирж (в десятичных значениях)
# ================================
exchange_fees = {
    "Bybit": 0.001,   # 0.10%
    "OKX": 0.001,     # 0.10%
    "BingX": 0.001,   # 0.10%
    "MEXC": 0.002,    # 0.20%
    "HTX": 0.002,     # 0.20%
    "Gate.io": 0.002, # 0.20%
    "KuCoin": 0.001,  # 0.10%
    "Bitget": 0.001   # 0.10%
}

# ================================
# Словарь для определения блокчейн-сети по символу монеты
# ================================
default_network_mapping = {
    "BTC": "Bitcoin",
    "ETH": "Ethereum",
    "PI": "N/A",
    "SOL": "Solana",
    "BGB": "N/A",
    "KAITO": "N/A",
    "XRP": "XRP Ledger",
    "DOGE": "Dogecoin",
    "IP": "N/A",
    "SUI": "Sui",
    "USDC": "Ethereum",
    "RAY": "Solana",
    "LTC": "Litecoin",
    "HBAR": "Hedera Hashgraph",
    "OM": "N/A",
    "BAN": "N/A",
    "TRUMP": "N/A",
    "WIF": "N/A",
    "PEPE": "Ethereum",
    "ONDO": "N/A",
    "RUNE": "THORChain",
    "TAO": "N/A",
    "HYPE": "N/A",
    "BERA": "N/A",
    "LINK": "Ethereum",
    "TRX": "TRON",
    "ADA": "Cardano",
    "VIRTUAL": "N/A",
    "WLD": "N/A",
    "ENA": "N/A",
    "AVAX": "Avalanche",
    "VINE": "N/A",
    "JUP": "Solana",
    "AIXBT": "N/A",
    "BNB": "BNB Chain",
    "MKR": "Ethereum",
    "MX": "N/A",
    "APT": "Aptos",
    "MOODENG": "N/A",
    "FET": "Ethereum",
    "PNUT": "N/A",
    "PENGU": "N/A",
    "SEI": "Sei",
    "FIL": "Filecoin",
    "FARTCOIN": "N/A",
    "X": "N/A",
    "ETHFI": "N/A",
    "ANIME": "N/A",
    "SHIB": "Ethereum",
    "CGPT": "N/A",
    "OP": "Optimism",
    "GPU1": "N/A",
    "TIA": "N/A",
    "NEAR": "Near",
    "XLM": "Stellar",
    "NC": "N/A",
    "ARB": "Arbitrum",
    "XRP5L": "N/A",
    "ORDI": "N/A",
    "TON": "TON",
    "SOLV": "N/A",
    "ICP": "Internet Computer",
    "KAS": "N/A",
    "AI16Z": "N/A",
    "DOT": "Polkadot",
    "NOT": "N/A",
    "ARKM": "N/A",
    "KITEAI": "N/A",
    "EIGEN": "N/A",
    "BNX": "N/A",
    "DOGE5S": "N/A",
    "VANA": "N/A",
    "HIPPO": "N/A",
    "AAVE": "Ethereum",
    "DOGE5L": "N/A",
    "LINK5L": "N/A",
    "AVAAI": "N/A",
    "LDO": "Ethereum",
    "ETH5L": "N/A",
    "ARC": "N/A",
    "INJ": "Ethereum",
    "FDUSD": "N/A",
    "ETC": "Ethereum Classic",
    "WBTC": "Ethereum",
    "XCN": "N/A",
    "UNI": "Ethereum",
    "XMR": "Monero",
    "VR": "N/A",
    "METIS": "Metis",
    "ETH5S": "N/A",
    "AR": "N/A",
    "PYTH": "N/A",
    "SONIC": "N/A",
    "TURBO": "N/A",
    "STETH": "Ethereum",
    "AIC": "N/A",
    "VVV": "N/A",
    "SWARMS": "N/A",
    "POPCAT": "N/A",
    "XRP5S": "N/A",
    "MAJOR": "N/A",
    "GALA5L": "N/A",
    "CFX": "Conflux",
    "ULTIMA": "N/A",
    "OCB": "N/A",
    "FTT": "Ethereum",
    "ALGO": "Algorand",
    "BIO": "N/A",
    "TOMI": "N/A",
    "BCH": "Bitcoin Cash",
    "CRO": "Cronos",
    "ACT": "N/A",
    "SHELL": "N/A",
    "ZK": "N/A",
    "BRISE": "N/A",
    "CRV": "Ethereum",
    "BONK": "Solana",
    "CAKE": "BNB Chain",
    "WMTX": "N/A",
    "DIAM": "N/A",
    "DRIFT": "Ethereum",
    "POL": "Polkadot",
    "KOMA": "N/A",
    "JELLYJELLY": "N/A",
    "ORCA": "Solana",
    "GMT": "Ethereum",
    "PRCL": "N/A",
    "FLOKI": "Ethereum",
    "LINK5S": "N/A",
    "STX": "Stacks",
    "SUPRA": "N/A",
    "MOVE": "N/A",
    "EOS": "EOSIO",
    "APE": "Ethereum",
    "RENDER": "Ethereum",
    "NEIROCTO": "N/A",
    "ATOM": "Cosmos",
    "AUCTION": "N/A",
    "QUAI": "N/A",
    "SLERF": "N/A",
    "ZBCN": "N/A",
    "ENS": "Ethereum",
    "LTC5L": "N/A",
    "DYDX": "Ethereum",
    "XCN": "N/A",
    "GALA": "Ethereum",
    "AB": "N/A",
    "SUNDOG": "N/A",
    "XAI": "N/A",
    "ACH": "N/A",
    "UNI5L": "N/A",
    "CORE": "N/A",
    "MEME": "N/A",
    "SOLAYER": "N/A",
    "ME": "N/A",
    "TFUEL": "Theta Fuel",
    "SERAPH": "N/A",
    "BANANA": "BNB Chain",
    "DOG": "N/A",
    "MAX": "N/A",
    "ZRO": "N/A",
    "ASTR": "AstraChain",
    "W": "N/A",
    "SAFE": "N/A",
    "SAND": "Ethereum",
    "SHIB5L": "N/A",
    "LAYER": "N/A",
    "VELO": "N/A",
    "PEAQ": "N/A",
    "PLUME": "N/A",
    "SSV": "N/A",
    "JTO": "N/A",
    "AIST": "N/A",
    "STPT": "N/A",
    "ALU": "N/A",
    "VET": "VeChain",
    "ZETA": "N/A",
    "GRASS": "N/A",
    "J": "N/A",
    "B3": "N/A",
    "XTZ": "Tezos",
    "MELANIA": "N/A",
    "COOKIE": "N/A",
    "BUZZ": "N/A",
    "FLR": "Flare",
    "AVA": "Avalanche",
    "LISTA": "N/A",
    "USUAL": "N/A",
    "CVC": "Ethereum",
    "XTER": "N/A",
    "CHZ": "Ethereum",
    "KAIA": "N/A",
    "RSR": "Ethereum",
    "ZEN": "Horizen",
    "IOTA": "IOTA",
    "NEO": "Neo",
    "SNX": "Ethereum",
    "AXS": "Ethereum",
    "BURGER": "BNB Chain",
    "TRB": "Ethereum",
    "SUSHI": "Ethereum",
    "ELON": "Ethereum",
    "DEGEN": "N/A",
    "ICE": "N/A",
    "SPELL": "Ethereum",
    "IMX": "Ethereum",
    "NMR": "Ethereum",
    "AGLD": "Ethereum",
    "GLM": "Ethereum",
    "FLOW": "Flow",
    "CSPR": "Casper",
    "ELA": "Elastos",
    "BAKE": "BNB Chain",
    "OSMO": "Osmosis",
    "AIOZ": "Ethereum",
    "KDA": "Kadena",
    "PENDLE": "Ethereum",
    "NEIRO": "N/A"
}

# ================================
# Функция получения цены с биржи через CCXT
# ================================
async def fetch_ticker_for_exchange(exchange, exchange_name, unified_symbol):
    if unified_symbol not in exchange.markets:
        print(f"{exchange_name} не торгует {unified_symbol}. Пропускаем.")
        return exchange_name, None
    market = exchange.markets[unified_symbol]
    raw_symbol = market.get("id")
    if not raw_symbol:
        base, quote = unified_symbol.split("/")
        raw_symbol = format_symbol(exchange_name, base, quote)
    try:
        ticker = await exchange.fetch_ticker(raw_symbol)
        price = ticker.get("last")
        print(f"{exchange_name} ({unified_symbol}) → raw: {raw_symbol}, цена: {price}")
        return exchange_name, price
    except RateLimitExceeded:
        print(f"{exchange_name} превысил лимит запросов для {unified_symbol}. Пропускаем.")
        return exchange_name, None
    except Exception as e:
        print(f"{exchange_name} ошибка для {unified_symbol} (raw: {raw_symbol}): {e}")
        return exchange_name, None

# ================================
# Функция определения блокчейн-сети для монеты через данные бирж и словарь
# ================================
async def get_network_for_coin(coin: str) -> str:
    symbol = f"{coin}/USDT"
    network = None
    for exchange_name, exchange in exchanges.items():
        if symbol in exchange.markets:
            market = exchange.markets[symbol]
            info = market.get("info", {})
            possible_keys = ["network", "platform", "chain", "contract", "contractAddress", "contract_address"]
            for key in possible_keys:
                if key in info and info[key]:
                    value = info[key]
                    if isinstance(value, str) and value.strip():
                        network = value.strip()
                        print(f"Получена сеть для {coin} через CCXT на {exchange_name}: {network}")
                        break
                    elif isinstance(value, dict) and value:
                        network = list(value.keys())[0]
                        print(f"Получена сеть для {coin} через CCXT (dict) на {exchange_name}: {network}")
                        break
        if network:
            break
    if not network or network == "N/A":
        network = default_network_mapping.get(coin.upper(), "N/A")
        if network != "N/A":
            print(f"Определена сеть для {coin} через словарь: {network}")
        else:
            print(f"Не удалось определить сеть для {coin}")
    return network

# ================================
# Функция проверки арбитражных возможностей как асинхронный генератор
# ================================
async def check_arbitrage():
    tickers_by_exchange = {}
    for exchange_name, exchange in exchanges.items():
        try:
            tickers = await exchange.fetch_tickers()
            tickers_by_exchange[exchange_name] = tickers
        except Exception as e:
            print(f"Ошибка получения тикеров от {exchange_name}: {e}")
    liquidity = {}
    for exchange_name, tickers in tickers_by_exchange.items():
        for symbol, ticker in tickers.items():
            if not symbol.endswith("/USDT"):
                continue
            base, _ = symbol.split("/")
            vol = ticker.get("quoteVolume") or ticker.get("baseVolume") or 0
            liquidity[base] = liquidity.get(base, 0) + vol
    top_coins = sorted(liquidity, key=liquidity.get, reverse=True)[:300]
    print("Топ 300 ликвидных активов:", top_coins)
    for coin in top_coins:
        tasks = []
        symbol = f"{coin}/USDT"
        for exchange_name, exchange in exchanges.items():
            if symbol not in exchange.markets:
                continue
            tasks.append(fetch_ticker_for_exchange(exchange, exchange_name, symbol))
        if not tasks:
            continue
        results = await asyncio.gather(*tasks, return_exceptions=True)
        prices = {}
        for res in results:
            if isinstance(res, tuple):
                name, price = res
                if price is not None:
                    prices[name] = price
        if len(prices) < 2:
            continue
        exch_buy = min(prices, key=prices.get)
        exch_sell = max(prices, key=prices.get)
        price_buy = prices[exch_buy]
        price_sell = prices[exch_sell]
        spread = (price_sell - price_buy) / price_buy
        # Если спред больше 50%, сигнал не отправляем
        if spread >= MAX_SPREAD:
            continue
        if spread >= THRESHOLD:
            network = await get_network_for_coin(coin)
            moscow_tz = pytz.timezone("Europe/Moscow")
            moscow_time = datetime.now(moscow_tz).strftime("%H:%M:%S")
            fee_buy = exchange_fees.get(exch_buy, 0)
            fee_sell = exchange_fees.get(exch_sell, 0)
            net_spread = spread - (fee_buy + fee_sell)
            if net_spread < 0:
                net_spread = 0
            msg = f"🕒 Время МСК: {moscow_time}\n\n" \
                  f"✅ Покупка на {exch_buy}:\n" \
                  f"🔸 Актив: {coin}\n" \
                  f"💵 Цена: {price_buy}\n\n" \
                  f"🛒 Продажа на {exch_sell}:\n" \
                  f"🔹 Актив: {coin}\n" \
                  f"💵 Цена: {price_sell}\n\n" \
                  f"📈 Спред (после комиссий): {net_spread*100:.2f}%\n"
            if network != "N/A":
                msg += f"🔄 Сеть для перевода ({coin}): {network}"
            yield msg

# ================================
# Инициализация бирж через CCXT
# ================================
exchanges = {
    "Bybit": ccxt.bybit(),
    "OKX": ccxt.okx(),
    "BingX": ccxt.bingx(),
    "MEXC": ccxt.mexc(),
    "HTX": ccxt.huobijp(),
    "Gate.io": ccxt.gateio(),
    "KuCoin": ccxt.kucoin(),
    "Bitget": ccxt.bitget()
}

# ================================
# Обработчик команды /start для регистрации пользователя
# ================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    registered_users.add(chat_id)
    await update.message.reply_text("🤖 Арбитраж-бот запущен! Вы успешно подписались на уведомления.")

# ================================
# Функция рассылки уведомлений – сигнал отправляется сразу, как только обнаружен
# ================================
async def job_arbitrage(context: ContextTypes.DEFAULT_TYPE) -> None:
    async for msg in check_arbitrage():
        for chat_id in registered_users:
            try:
                await context.bot.send_message(chat_id=chat_id, text=msg)
            except Exception as e:
                print(f"Ошибка отправки сообщения пользователю {chat_id}: {e}")

# ================================
# Основная функция запуска бота и арбитражного мониторинга
# ================================
async def main():
    global exchanges
    loaded_exchanges = {}
    for exchange_name, exchange in list(exchanges.items()):
        try:
            await exchange.load_markets()
            loaded_exchanges[exchange_name] = exchange
        except Exception as e:
            print(f"Ошибка загрузки маркетов для {exchange_name}: {e}. Биржа будет исключена.")
    exchanges = loaded_exchanges

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    
    app.job_queue.run_repeating(job_arbitrage, interval=10, first=0)
    
    try:
        await app.run_polling()
    finally:
        await asyncio.gather(*(exchange.close() for exchange in exchanges.values()), return_exceptions=True)
        print("Все соединения закрыты.")

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
