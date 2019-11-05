import argparse
from infcommon import logger
from infrabbitmq import factory as infrabbitmq_factory


def main(queue_name):
    infrabbitmq_factory.configure_pika_logger_to_error()

    rabbitmq_client = infrabbitmq_factory.no_singleton_rabbitmq_client()
    logger.info(f'Deleting queue: {queue_name}')
    rabbitmq_client.queue_delete(queue_name=queue_name)
    logger.info(f'Deleted queue: {queue_name}')


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('queue_name')
        args = parser.parse_args()

        main(queue_name=args.queue_name)
    except Exception as exc:
        logger.critical(f'Queue delete Fails. exc_type: {type(exc)} exc: {exc}',
                        exc_info=True)
