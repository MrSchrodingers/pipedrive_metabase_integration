import logging
import sys
import structlog
import os 


def setup_logging(level=logging.INFO, force_json=False):
    """
    Configura o logging padrão do Python e o structlog para uma saída
    estruturada unificada (JSON por padrão em ambientes não-TTY ou se forçado,
    ConsoleRenderer caso contrário).

    Logs de bibliotecas padrão serão capturados e processados pelo structlog.

    Args:
        level: O nível de log mínimo (ex: logging.INFO, logging.DEBUG).
        force_json (bool): Se True, força a saída JSON mesmo em TTY.
                           Útil para testes ou ambientes específicos.
    """
    # 1. Configurar processadores compartilhados do structlog
    shared_processors = [
        # Adiciona contexto de structlog.contextvars e thread locals
        structlog.contextvars.merge_contextvars,
        structlog.threadlocal.merge_threadlocal,
        # Adiciona o nome do logger e nível de log padrão se não existirem
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        # Permite logar argumentos posicionais como {'positional_args': [1, 2]}
        structlog.stdlib.PositionalArgumentsFormatter(),
        # Adiciona timestamp ISO 8601
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        # Renderiza stack traces e exceptions
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        # Decodifica bytes para unicode
        structlog.processors.UnicodeDecoder(),
        # IMPORTANTE: Processador para preparar logs vindos do logging padrão
        # Deve vir ANTES do renderer final
        structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
    ]

    # 2. Escolher o Renderer Final (JSON ou Console)
    log_renderer = None
    is_tty = sys.stdout.isatty()
    log_format = os.getenv("LOG_FORMAT", "CONSOLE" if is_tty else "JSON").upper()

    if not force_json and log_format == "CONSOLE":
        # Renderer colorido para desenvolvimento local/TTY
        log_renderer = structlog.dev.ConsoleRenderer(colors=True) # Ativar cores
        log_level_key = "level" 
    else:
        # Renderer JSON para produção/logs agregados ou se forçado
        # Adiciona 'event' como chave principal da mensagem
        # keys_order garante ordem (opcional mas útil)
        log_renderer = structlog.processors.JSONRenderer(
            sort_keys=False, # Mais rápido sem ordenar
            keys_order=["timestamp", "level", "logger", "event", "flow_run_id"]
        )
        log_level_key = "level"

    # 3. Configurar structlog para usar os processadores e o renderer
    structlog.configure(
        processors=shared_processors + [
            log_renderer
        ],
        # Usa a factory padrão do logging para criar loggers
        logger_factory=structlog.stdlib.LoggerFactory(),
        # Permite que loggers padrão usem processadores do structlog
        wrapper_class=structlog.stdlib.BoundLogger,
        # Cacheia o logger na primeira utilização para performance
        cache_logger_on_first_use=True,
    )

    # 4. Configurar o logging padrão para usar o structlog
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    # Limpar handlers existentes para evitar duplicação/formatos mistos
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(level)

    # Configurar níveis de log para bibliotecas verbosas 
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    # Log de confirmação usando structlog 
    log = structlog.get_logger("logging_setup")
    log.info("Logging configured.", level=logging.getLevelName(level), format=log_format)