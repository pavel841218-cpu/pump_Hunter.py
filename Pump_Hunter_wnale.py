#!/usr/bin/env python3
"""
Pump Hunter Pro v3.0 - Продвинутый детектор пампа на Binance Futures
Отслеживает аномалии Открытого Интереса (OI) для предсказания резких движений цены

Особенности:
- Расширенный список токенов (Solana, AI, RWA, основные шорт-сквиз цели)
- Адаптивные пороги срабатывания на основе волатильности
- Защита от спама алертами
- Умная обработка rate-limit Binance
- Система самодиагностики и самовосстановления
- Полная поддержка Render (анти-сон, веб-сервер)
"""

import os
import sys
import asyncio
import logging
import re
import time
import statistics
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime, timezone
from dataclasses import dataclass, field
from enum import Enum
from collections import deque

import httpx
from aiohttp import web
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.exceptions import TelegramAPIError

# ═══════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ И НАСТРОЙКИ
# ═══════════════════════════════════════════════════════════

class Blockchain(Enum):
    """Типы блокчейнов для корректных ссылок на эксплореры"""
    SOLANA = "solana"
    ETHEREUM = "ethereum"
    BINANCE = "binance"
    RIPPLE = "ripple"
    CARDANO = "cardano"
    NEAR = "near"
    AVALANCHE = "avalanche"
    OTHER = "other"

@dataclass
class TokenConfig:
    """Конфигурация отслеживаемого токена"""
    symbol: str                    # Торговый символ на Binance (например, "SOLUSDT")
    clean_name: str                # Чистое имя токена (например, "SOL")
    blockchain: Blockchain         # Блокчейн для ссылок на эксплореры
    category: str                  # Категория токена (мем, AI, RWA, L1, etc.)
    priority: int = 1              # Приоритет проверки (1 - высокий, 3 - низкий)
    base_threshold: float = 1.5    # Базовый порог срабатывания в %
    min_volume_24h: float = 1_000_000  # Минимальный объём для алерта ($)
    cooldown_minutes: int = 5      # Минимальный интервал между алертами
    
    @property
    def tradingview_url(self) -> str:
        """Ссылка на график TradingView"""
        return f"https://ru.tradingview.com/chart/?symbol=BINANCE:{self.symbol}.P"
    
    @property
    def explorer_url(self) -> Optional[str]:
        """Ссылка на блокчейн-эксплорер"""
        explorers = {
            Blockchain.SOLANA: f"https://solscan.io/token/{self.clean_name}",
            Blockchain.ETHEREUM: f"https://etherscan.io/token/{self.clean_name}",
            Blockchain.BINANCE: f"https://bscscan.com/token/{self.clean_name}",
            Blockchain.RIPPLE: "https://xrpscan.com/",
            Blockchain.CARDANO: "https://cardanoscan.io/",
            Blockchain.NEAR: f"https://nearblocks.io/address/{self.clean_name}",
            Blockchain.AVALANCHE: f"https://snowtrace.io/token/{self.clean_name}",
        }
        return explorers.get(self.blockchain)
    
    @property
    def dex_url(self) -> Optional[str]:
        """Ссылка на DEX для быстрой покупки"""
        dexes = {
            Blockchain.SOLANA: f"https://jup.ag/swap/USDC-{self.clean_name}",
            Blockchain.ETHEREUM: f"https://app.uniswap.org/swap?outputCurrency={self.clean_name}",
            Blockchain.BINANCE: f"https://pancakeswap.finance/swap?outputCurrency={self.clean_name}",
            Blockchain.AVALANCHE: f"https://traderjoexyz.com/avalanche/trade?outputCurrency={self.clean_name}",
        }
        return dexes.get(self.blockchain)

# ═══════════════════════════════════════════════════════════
# РАСШИРЕННЫЙ СПИСОК ТОКЕНОВ ДЛЯ МОНИТОРИНГА
# ═══════════════════════════════════════════════════════════

WATCH_TOKENS: List[TokenConfig] = [
    # Solana-экосистема (высокий приоритет - частые пампы)
    TokenConfig("SOLUSDT", "SOL", Blockchain.SOLANA, "L1", priority=1, base_threshold=1.2),
    TokenConfig("WIFUSDT", "WIF", Blockchain.SOLANA, "Meme", priority=1, base_threshold=2.0),
    TokenConfig("BONKUSDT", "BONK", Blockchain.SOLANA, "Meme", priority=2, base_threshold=2.5),
    TokenConfig("POPCATUSDT", "POPCAT", Blockchain.SOLANA, "Meme", priority=2, base_threshold=3.0),
    
    # Основные цели для шорт-сквизов (высокая ликвидность)
    TokenConfig("XRPUSDT", "XRP", Blockchain.RIPPLE, "L1", priority=1, base_threshold=1.0),
    TokenConfig("DOGEUSDT", "DOGE", Blockchain.OTHER, "Meme", priority=1, base_threshold=1.5),
    TokenConfig("ADAUSDT", "ADA", Blockchain.CARDANO, "L1", priority=2, base_threshold=1.5),
    
    # AI-сектор (высокая волатильность)
    TokenConfig("NEARUSDT", "NEAR", Blockchain.NEAR, "AI/L1", priority=1, base_threshold=2.0),
    TokenConfig("FETUSDT", "FET", Blockchain.ETHEREUM, "AI", priority=1, base_threshold=2.5),
    
    # Высоколиквидная альта (институциональные потоки)
    TokenConfig("LINKUSDT", "LINK", Blockchain.ETHEREUM, "Oracle", priority=2, base_threshold=1.5),
    TokenConfig("AVAXUSDT", "AVAX", Blockchain.AVALANCHE, "L1", priority=2, base_threshold=1.8),
    
    # RWA-сектор
    TokenConfig("ONDOUSDT", "ONDO", Blockchain.ETHEREUM, "RWA", priority=2, base_threshold=3.0),
    TokenConfig("PENDLEUSDT", "PENDLE", Blockchain.ETHEREUM, "DeFi/RWA", priority=3, base_threshold=3.0),
]

# Сортировка по приоритету для оптимального порядка проверки
WATCH_TOKENS.sort(key=lambda x: (x.priority, -x.base_threshold))

# ═══════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ ИЗ ПЕРЕМЕННЫХ ОКРУЖЕНИЯ
# ═══════════════════════════════════════════════════════════

def validate_config() -> dict:
    """Расширенная валидация конфигурации"""
    config = {}
    
    token = os.getenv("PUMP_BOT_TOKEN")
    if not token:
        logging.critical("❌ PUMP_BOT_TOKEN не найден в переменных окружения!")
        sys.exit(1)
    
    token_pattern = re.compile(r'^\d{8,10}:[A-Za-z0-9_-]{35,}$')
    if not token_pattern.match(token):
        logging.critical("❌ Неверный формат токена Telegram бота!")
        sys.exit(1)
    config['TOKEN'] = token
    
    chat_id_str = os.getenv("PUMP_CHAT_ID")
    if not chat_id_str:
        logging.critical("❌ PUMP_CHAT_ID не установлен!")
        sys.exit(1)
    
    try:
        chat_id = int(chat_id_str.strip())
        if chat_id <= 0:
            raise ValueError("Chat ID должен быть положительным")
        config['CHAT_ID'] = chat_id
    except ValueError as e:
        logging.critical(f"❌ Ошибка PUMP_CHAT_ID: {e}")
        sys.exit(1)
    
    self_url = os.getenv("PUMP_SELF_URL")
    if not self_url:
        logging.warning("⚠️ PUMP_SELF_URL не установлен. Защита от сна Render отключена.")
        config['SELF_URL'] = None
    else:
        if not self_url.startswith(('http://', 'https://')):
            logging.warning(f"⚠️ Подозрительный PUMP_SELF_URL: {self_url}")
        config['SELF_URL'] = self_url
    
    port_str = os.getenv("PORT", "7861")
    try:
        port = int(port_str)
        config['PORT'] = port
    except ValueError:
        logging.warning(f"⚠️ Неверный PORT: {port_str}, использую 7861")
        config['PORT'] = 7861
    
    config['MIN_ALERT_INTERVAL'] = int(os.getenv("MIN_ALERT_INTERVAL", "300"))
    config['CHECK_INTERVAL'] = int(os.getenv("CHECK_INTERVAL", "60"))
    config['PING_INTERVAL'] = int(os.getenv("PING_INTERVAL", "240"))  # Оптимально 4 минуты для Render
    
    return config

# ═══════════════════════════════════════════════════════════
# НАСТРОЙКА ЛОГИРОВАНИЯ
# ═══════════════════════════════════════════════════════════

def setup_logging():
    """Профессиональная настройка логирования"""
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG)
    console.setFormatter(formatter)
    
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(console)
    
    for lib in ['httpx', 'httpcore', 'aiogram', 'aiohttp']:
        logging.getLogger(lib).setLevel(logging.WARNING)
    
    logger = logging.getLogger('PumpHunter')
    logger.info("🚀 Система логирования инициализирована")
    
    return logger

logger = setup_logging()

# ═══════════════════════════════════════════════════════════
# МОДЕЛИ ДАННЫХ
# ═══════════════════════════════════════════════════════════

@dataclass
class OIMetrics:
    """Метрики открытого интереса"""
    current_oi: float
    previous_oi: float
    change_percent: float
    change_absolute: float
    timestamp: float
    symbol: str
    
    @property
    def is_significant(self) -> bool:
        """Проверка на значимость изменения"""
        return abs(self.change_percent) >= 0.1

@dataclass
class TokenState:
    """Состояние отслеживания токена"""
    token: TokenConfig
    oi_history: deque = field(default_factory=lambda: deque(maxlen=50))
    alerts_sent: int = 0
    last_alert_time: float = 0
    last_oi_value: float = 0
    consecutive_errors: int = 0
    is_active: bool = True
    
    def can_alert(self, cooldown_seconds: int = 300) -> bool:
        """Проверка возможности отправки алерта"""
        if not self.is_active:
            return False
        
        current_time = time.time()
        if current_time - self.last_alert_time < cooldown_seconds:
            return False
        
        return True
    
    def record_alert(self):
        """Запись факта отправки алерта"""
        self.alerts_sent += 1
        self.last_alert_time = time.time()
    
    def record_error(self):
        """Запись ошибки с возможной деактивацией"""
        self.consecutive_errors += 1
        if self.consecutive_errors >= 10:
            logger.error(f"🔴 Токен {self.token.symbol} деактивирован из-за ошибок")
            self.is_active = False
    
    def record_success(self):
        """Сброс счётчика ошибок"""
        self.consecutive_errors = 0

# ═══════════════════════════════════════════════════════════
# КЛИЕНТ BINANCE API
# ═══════════════════════════════════════════════════════════

class BinanceOIClient:
    """Асинхронный клиент для работы с Binance Futures API"""
    
    BASE_URL = "https://fapi.binance.com"
    MAX_RETRIES = 3
    
    def __init__(self):
        self.client: Optional[httpx.AsyncClient] = None
        self.rate_limit_remaining = 1200
        self.request_count = 0
        self.error_count = 0
        
    async def start(self):
        """Инициализация HTTP клиента"""
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=5.0, read=10.0, write=5.0, pool=5.0),
            limits=httpx.Limits(max_keepalive_connections=10, max_connections=20, keepalive_expiry=30.0),
            headers={'Accept': 'application/json', 'User-Agent': 'PumpHunterPro/3.0'}
        )
        logger.info("✅ Binance API клиент инициализирован")
    
    async def fetch_open_interest(self, symbol: str) -> Optional[float]:
        """Получение открытого интереса с защитой от ошибок"""
        if not self.client:
            raise RuntimeError("Клиент не инициализирован")
        
        url = f"{self.BASE_URL}/fapi/v1/openInterest"
        
        for attempt in range(self.MAX_RETRIES):
            try:
                if self.rate_limit_remaining < 10:
                    await asyncio.sleep(10)
                
                response = await self.client.get(url, params={'symbol': symbol})
                self.request_count += 1
                
                if 'X-Mbx-Used-Weight-1m' in response.headers:
                    used_weight = int(response.headers['X-Mbx-Used-Weight-1m'])
                    self.rate_limit_remaining = 1200 - used_weight
                
                if response.status_code == 200:
                    data = response.json()
                    oi = float(data.get('openInterest', 0))
                    if oi <= 0:
                        logger.warning(f"⚠️ Нулевой OI для {symbol}")
                        return None
                    return oi
                    
                elif response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 10))
                    logger.warning(f"⏳ Rate-limit для {symbol}. Ожидание {retry_after}с")
                    await asyncio.sleep(retry_after)
                    
                elif response.status_code >= 500:
                    logger.error(f"🔴 Ошибка сервера Binance: {response.status_code}")
                    await asyncio.sleep(2 ** attempt)
                else:
                    logger.warning(f"⚠️ Статус {response.status_code} для {symbol}")
                    return None
                    
            except httpx.TimeoutException:
                logger.warning(f"⏰ Таймаут для {symbol} (попытка {attempt+1})")
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"❌ Ошибка запроса {symbol}: {type(e).__name__}")
                self.error_count += 1
                await asyncio.sleep(1)
        return None
    
    async def stop(self):
        """Закрытие клиента"""
        if self.client:
            await self.client.aclose()
            logger.info(f"📊 Статистика API: {self.request_count} запросов, {self.error_count} ошибок")

# ═══════════════════════════════════════════════════════════
# ТРЕКЕР ОТКРЫТОГО ИНТЕРЕСА
# ═══════════════════════════════════════════════════════════

class OITracker:
    """Отслеживание изменений открытого интереса с адаптивными порогами"""
    
    def __init__(self, tokens: List[TokenConfig]):
        self.states: Dict[str, TokenState] = {
            t.symbol: TokenState(token=t) for t in tokens
        }
        self._lock = asyncio.Lock()
    
    async def update(self, symbol: str, oi_value: float) -> Optional[OIMetrics]:
        """Обновление OI и расчёт метрик"""
        async with self._lock:
            if symbol not in self.states:
                return None
            
            state = self.states[symbol]
            previous_oi = state.last_oi_value if state.last_oi_value > 0 else oi_value
            
            metrics = OIMetrics(
                current_oi=oi_value,
                previous_oi=previous_oi,
                change_percent=((oi_value - previous_oi) / previous_oi * 100) if previous_oi > 0 else 0,
                change_absolute=oi_value - previous_oi,
                timestamp=time.time(),
                symbol=symbol
            )
            
            state.last_oi_value = oi_value
            state.oi_history.append(oi_value)
            state.record_success()
            
            return metrics
    
    async def should_alert(self, metrics: OIMetrics, min_interval: int = 300) -> bool:
        """Проверка необходимости алерта с адаптивным порогом"""
        if not metrics or not metrics.is_significant:
            return False
        
        async with self._lock:
            state = self.states.get(metrics.symbol)
            if not state or not state.is_active:
                return False
            
            if not state.can_alert(min_interval):
                return False
            
            threshold = self._calculate_threshold(state)
            
            if metrics.change_percent >= threshold:
                state.record_alert()
                logger.info(f"🚨 Алерт #{state.alerts_sent} для {metrics.symbol}: +{metrics.change_percent:.2f}% (порог: {threshold:.2f}%)")
                return True
            return False
    
    def _calculate_threshold(self, state: TokenState) -> float:
        """Расчёт адаптивного порога на основе истории"""
        base = state.token.base_threshold
        if len(state.oi_history) < 5:
            return base
        
        changes = []
        history = list(state.oi_history)
        for i in range(1, len(history)):
            if history[i-1] > 0:
                change = abs((history[i] - history[i-1]) / history[i-1] * 100)
                changes.append(change)
        
        if not changes:
            return base
        
        mean_change = statistics.mean(changes)
        std_change = statistics.stdev(changes) if len(changes) > 1 else 0
        adaptive = mean_change + (2 * std_change)
        return min(max(adaptive, base), base * 3)
    
    def get_active_tokens(self) -> List[str]:
        return [s for s, state in self.states.items() if state.is_active]
    
    def get_stats(self) -> dict:
        return {
            'total_alerts': sum(s.alerts_sent for s in self.states.values()),
            'active_tokens': len(self.get_active_tokens()),
            'total_tokens': len(self.states),
            'by_token': {
                sym: {'alerts': state.alerts_sent, 'active': state.is_active, 'last_oi': state.last_oi_value}
                for sym, state in self.states.items()
            }
        }

# ═══════════════════════════════════════════════════════════
# ФОРМАТТЕР СООБЩЕНИЙ
# ═══════════════════════════════════════════════════════════

class AlertFormatter:
    """Форматирование алертов для Telegram"""
    
    @staticmethod
    def format_pump_alert(token: TokenConfig, metrics: OIMetrics, state: TokenState) -> str:
        category_emoji = {
            "Meme": "🐸", "AI": "🤖", "AI/L1": "🧠", "L1": "⛓️", 
            "RWA": "🏦", "DeFi": "💱", "DeFi/RWA": "💎", "Oracle": "🔮"
        }.get(state.token.category, "📊")
        
        if metrics.change_percent >= 5: strength = "🔴 ЭКСТРЕМАЛЬНЫЙ"
        elif metrics.change_percent >= 3: strength = "🟠 СИЛЬНЫЙ"
        elif metrics.change_percent >= 2: strength = "🟡 СРЕДНИЙ"
        else: strength = "🟢 СЛАБЫЙ"
        
        return f"""
{category_emoji} <b>PUMP DETECTED | {state.token.category}</b>

🔥 Монета: <code>{state.token.clean_name}</code> ({state.token.symbol})
📊 Сигнал: {strength}
📈 Рост OI: <b>+{metrics.change_percent:.2f}%</b> за 1 мин
💰 Текущий OI: <code>${metrics.current_oi:,.0f}</code>
📈 Изменение: <code>+${metrics.change_absolute:,.0f}</code>

🔍 Анализ:
• Блокчейн: {state.token.blockchain.value.title()}
• Порог: {state.token.base_threshold:.1f}% (базовый)
• Алертов сегодня: {state.alerts_sent}

⚡️ <b>Быстрые действия:</b>
• <a href='{state.token.tradingview_url}'>📊 График TradingView</a>
• <a href='{state.token.explorer_url or "#"}'>🔗 Эксплорер</a>
• <a href='{state.token.dex_url or "#"}'>💱 Быстрая покупка</a>
• <a href='https://www.binance.com/ru/futures/{state.token.symbol}'>📈 Binance Futures</a>

🕐 {datetime.now().strftime('%H:%M:%S')} | #{state.alerts_sent}
""".strip()
    
    @staticmethod
    def format_status(stats: dict, uptime: float) -> str:
        hours = uptime / 3600
        active_lines = [f"• {sym}: {data['alerts']} алертов" for sym, data in stats['by_token'].items() if data['alerts'] > 0]
        tokens_log = "\n".join(active_lines) if active_lines else "• Алертов пока не было"
        return f"""
📊 <b>Статус Pump Hunter Pro</b>

⏱ Аптайм: {hours:.1f} часов
🎯 Активных токенов: {stats['active_tokens']}/{stats['total_tokens']}
🚨 Всего алертов: {stats['total_alerts']}

📋 <b>По токенам:</b>
{tokens_log}

🔄 Следующая проверка через 60 сек
""".strip()

# ═══════════════════════════════════════════════════════════
# ОСНОВНОЙ ОХОТНИК
# ═══════════════════════════════════════════════════════════

class PumpHunter:
    """Главный класс охотника за пампами"""
    
    def __init__(self, bot: Bot, chat_id: int, config: dict):
        self.bot = bot
        self.chat_id = chat_id
        self.config = config
        
        self.oi_client = BinanceOIClient()
        self.oi_tracker = OITracker(WATCH_TOKENS)
        self.formatter = AlertFormatter()
        
        self.is_running = False
        self.start_time = time.time()
        self.cycles_completed = 0
        self.pumps_detected = 0
        
        self._hunter_task: Optional[asyncio.Task] = None
        self._pinger_task: Optional[asyncio.Task] = None
    
    async def start(self):
        logger.info("🎯 Pump Hunter Pro запускается...")
        await self.oi_client.start()
        self.is_running = True
        self.start_time = time.time()
        
        self._hunter_task = asyncio.create_task(self._hunting_loop())
        if self.config.get('SELF_URL'):
            self._pinger_task = asyncio.create_task(self._ping_loop())
        
        logger.info(f"✅ Мониторинг {len(WATCH_TOKENS)} токенов активирован")
        await self.send_startup_message()
    
    async def _hunting_loop(self):
        logger.info("🔍 Цикл мониторинга запущен")
        while self.is_running:
            cycle_start = time.time()
            try:
                active_symbols = self.oi_tracker.get_active_tokens()
                tasks = [asyncio.create_task(self._check_token(symbol)) for symbol in active_symbols]
                await asyncio.gather(*tasks, return_exceptions=True)
                
                self.cycles_completed += 1
                cycle_duration = time.time() - cycle_start
                logger.debug(f"✅ Цикл #{self.cycles_completed} завершён за {cycle_duration:.1f}с")
                
                check_interval = self.config.get('CHECK_INTERVAL', 60)
                sleep_time = max(check_interval - cycle_duration, 30)
                await asyncio.sleep(sleep_time)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"💥 Критическая ошибка цикла: {e}", exc_info=True)
                await asyncio.sleep(60)
    
    async def _check_token(self, symbol: str):
        try:
            oi_value = await self.oi_client.fetch_open_interest(symbol)
            if oi_value is None:
                state = self.oi_tracker.states.get(symbol)
                if state: state.record_error()
                return
            
            metrics = await self.oi_tracker.update(symbol, oi_value)
            if not metrics: return
            
            min_interval = self.config.get('MIN_ALERT_INTERVAL', 300)
            if await self.oi_tracker.should_alert(metrics, min_interval):
                await self._send_alert(symbol, metrics)
                self.pumps_detected += 1
        except Exception as e:
            logger.error(f"❌ Ошибка проверки {symbol}: {e}")
    
    async def _send_alert(self, symbol: str, metrics: OIMetrics):
        try:
            token_config = next((t for t in WATCH_TOKENS if t.symbol == symbol), None)
            state = self.oi_tracker.states.get(symbol)
            if not token_config or not state: return
            
            message = self.formatter.format_pump_alert(token_config, metrics, state)
            await self.bot.send_message(chat_id=self.chat_id, text=message, parse_mode="HTML", disable_web_page_preview=True)
            logger.info(f"📤 Алерт отправлен для {symbol}: +{metrics.change_percent:.2f}%")
        except TelegramAPIError as e:
            logger.error(f"🚫 Ошибка Telegram API: {e}")
        except Exception as e:
            logger.error(f"💥 Ошибка отправки алерта: {e}")
    
    async def _ping_loop(self):
        url = self.config['SELF_URL']
        interval = self.config.get('PING_INTERVAL', 240)
        logger.info(f"🔄 Система анти-сна активирована (интервал: {interval}с)")
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            while self.is_running:
                try:
                    response = await client.get(url)
                    if response.status_code == 200: logger.debug("💚 Самопинг успешен")
                    else: logger.warning(f"⚠️ Статус пинга: {response.status_code}")
                except Exception as e:
                    logger.error(f"💔 Ошибка самопинга: {e}")
                await asyncio.sleep(interval)
    
    async def send_startup_message(self):
        try:
            msg = f"""
🚀 <b>Pump Hunter Pro v3.0 запущен!</b>

📊 Мониторинг: {len(WATCH_TOKENS)} токенов
🎯 Категории: Meme, AI, RWA, L1, DeFi
⚡️ Интервал проверки: {self.config.get('CHECK_INTERVAL', 60)}с

📋 <b>Отслеживаемые токены:</b>
{chr(10).join(f"• {t.clean_name} ({t.category})" for t in sorted(WATCH_TOKENS, key=lambda x: x.priority))}

🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC
"""
            await self.bot.send_message(chat_id=self.chat_id, text=msg.strip(), parse_mode="HTML")
        except Exception as e:
            logger.error(f"Ошибка отправки стартового сообщения: {e}")
    
    async def get_status_message(self) -> str:
        uptime = time.time() - self.start_time
        stats = self.oi_tracker.get_stats()
        return self.formatter.format_status(stats, uptime)
    
    async def stop(self):
        logger.info("🛑 Остановка Pump Hunter Pro...")
        self.is_running = False
        for task in [self._hunter_task, self._pinger_task]:
            if task:
                task.cancel()
                try: await task
                except asyncio.CancelledError: pass
        await self.oi_client.stop()

# ═══════════════════════════════════════════════════════════
# ТЕЛЕГРАМ БОТ (ИНТЕРФЕЙС И УПРАВЛЕНИЕ)
# ═══════════════════════════════════════════════════════════

class TelegramBot:
    """Обработчик команд Telegram"""
    
    def __init__(self, token: str, hunter: PumpHunter):
        self.bot = Bot(token=token)
        self.dp = Dispatcher()
        self.hunter = hunter
        self._setup_handlers()
    
    def _setup_handlers(self):
        @self.dp.message(Command("start"))
        async def cmd_start(message: Message):
            await message.answer(
                "🎯 <b>Pump Hunter Pro</b> активен!\n\n"
                "Отслеживаю аномалии открытого интереса на Binance Futures.\n\n"
                "Команды:\n"
                "/status - Статистика работы\n"
                "/tokens - Список токенов\n"
                "/help - Справка по проекту",
                parse_mode="HTML"
            )

        @self.dp.message(Command("status"))
        async def cmd_status(message: Message):
            if message.chat.id != self.hunter.chat_id: return
            status_text = await self.hunter.get_status_message()
            await message.answer(status_text, parse_mode="HTML")

        @self.dp.message(Command("tokens"))
        async def cmd_tokens(message: Message):
            if message.chat.id != self.hunter.chat_id: return
            tokens_lines = []
            for t in WATCH_TOKENS:
                state = self.hunter.oi_tracker.states.get(t.symbol)
                status_ico = "🟢" if state and state.is_active else "🔴"
                tokens_lines.append(f"{status_ico} <b>{t.clean_name}</b> ({t.category}) | Порог: {t.base_threshold}%")
            msg = "📋 <b>Список отслеживаемых токенов:</b>\n\n" + "\n".join(tokens_lines)
            await message.answer(msg, parse_mode="HTML")

        @self.dp.message(Command("help"))
        async def cmd_help(message: Message):
            msg = (
                "💡 <b>Как это работает?</b>\n\n"
                "Бот сканирует Binance Futures каждую минуту. Если Открытый Интерес (OI) "
                "резко увеличивается, это значит, что крупный игрок (кит) открывает скрытые позиции, "
                "используя лимитные ордера. Часто это происходит прямо перед импульсом цены (пампом).\n\n"
                "🎛 <b>Пороги адаптивные:</b> Бот рассчитывает текущую волатильность за последние "
                "50 минут и автоматически поднимает порог срабатывания во время сильной тряски на рынке, "
                "чтобы защитить тебя от ложного спама."
            )
            await message.answer(msg, parse_mode="HTML")

    async def start(self):
        logger.info("🤖 Поллинг Telegram-команд успешно запущен.")
        await self.dp.start_polling(self.bot)

# ═══════════════════════════════════════════════════════════
# ВЕБ-СЕРВЕР ДЛЯ RENDER (ЖИЗНЕННЫЙ ЦИКЛ ПРИЛОЖЕНИЯ)
# ═══════════════════════════════════════════════════════════

async def handle_health_check(request):
    return web.Response(text="Pump Hunter Pro: Live and Operational", status=200)

async def main():
    config = validate_config()
    bot = Bot(token=config['TOKEN'])
    
    hunter = PumpHunter(bot=bot, chat_id=config['CHAT_ID'], config=config)
    await hunter.start()
    
    tg_bot = TelegramBot(token=config['TOKEN'], hunter=hunter)
    
    app = web.Application()
    app.router.add_get('/', handle_health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    
    site = web.TCPSite(runner, '0.0.0.0', config['PORT'])
    await site.start()
    logger.info(f"🌐 HTTP Веб-сервер запущен на хосте 0.0.0.0:{config['PORT']}")
    
    await bot.delete_webhook(drop_pending_updates=True)
    
    try:
        await tg_bot.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Получен сигнал на остановку...")
    finally:
        await hunter.stop()
        await runner.cleanup()
        await bot.session.close()
        logger.info("💀 Программа полностью остановлена.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
