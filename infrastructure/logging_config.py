import logging
import sys
from pythonjsonlogger import jsonlogger
import structlog


def setup_logging(level=logging.INFO):
    """
    Configura o Python logging e o structlog para enviar tudo em JSON para stdout.
    Chamando este método apenas 1 vez no início da aplicação, 
    unifica logs de libs (logging) e logs próprios (structlog).
    """
    # ========== 1. Configuração do logging padrão em JSON ==========
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    handler = logging.StreamHandler(sys.stdout)

    formatter = jsonlogger.JsonFormatter(
        '%(asctime)s %(levelname)s %(name)s %(message)s'
    )
    handler.setFormatter(formatter)

    if root_logger.hasHandlers():
        root_logger.handlers.clear()
    root_logger.addHandler(handler)

    # ========== 2. Configuração do structlog para "empurrar" logs ao logging padrão ==========
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

