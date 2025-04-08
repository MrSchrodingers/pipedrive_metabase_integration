import logging
import sys
import structlog
import os 


def setup_logging(level=logging.INFO, force_json=False):
    """
    Configuração robusta e testada para structlog + logging padrão
    """
    # 1. Processadores comuns para todos os loggers
    common_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.threadlocal.merge_threadlocal,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]

    # 2. Escolher renderizador final
    if not force_json and sys.stdout.isatty():
        renderer = structlog.dev.ConsoleRenderer(colors=True)
    else:
        renderer = structlog.processors.JSONRenderer()

    # 3. Configuração completa do structlog
    structlog.configure(
        processors=common_processors + [renderer],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # 4. Configurar logging padrão para usar structlog
    formatter = structlog.stdlib.ProcessorFormatter(
        processor=renderer,
        foreign_pre_chain=common_processors,
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(level)

    # 5. Configurar níveis para bibliotecas ruidosas
    for lib in ["httpx", "httpcore", "urllib3"]:
        logging.getLogger(lib).setLevel(logging.WARNING)

    # Log inicial
    logger = structlog.get_logger(__name__)
    logger.info("Logging configurado com sucesso", level=level)