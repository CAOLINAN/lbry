#coding=utf-8
from lbrynet.core import log_support

import argparse
import logging.handlers

from twisted.internet import defer, reactor
from jsonrpc.proxy import JSONRPCProxy

from lbrynet import analytics
from lbrynet import conf
from lbrynet.core import utils, system_info
from lbrynet.daemon.DaemonServer import DaemonServer

log = logging.getLogger(__name__)


def test_internet_connection():
    return utils.check_connection(server='baidu.com')


def start():
    """The primary entry point for launching the daemon."""
    conf.initialize_settings()

    parser = argparse.ArgumentParser(description="Launch lbrynet-daemon")
    parser.add_argument(
        "--wallet",
        help="lbryum or ptc for testing, default lbryum",
        type=str,
        default=conf.settings['wallet']
    )
    parser.add_argument(
        "--http-auth", dest="useauth", action="store_true", default=conf.settings['use_auth_http']
    )
    parser.add_argument(
        '--quiet', dest='quiet', action="store_true",
        help='Disable all console output.'
    )
    parser.add_argument(
        '--verbose', nargs="*",
        help=('Enable debug output. Optionally specify loggers for which debug output '
              'should selectively be applied.')
    )
    parser.add_argument(
        '--version', action="store_true",
        help='Show daemon version and quit'
    )

    args = parser.parse_args()
    update_settings_from_args(args)  # 将use_auth_http(useauth)和wallet的值更新到配置类Config的self._data['cli']中

    if args.version:
        version = system_info.get_platform(get_ip=False)
        version['installation_id'] = conf.settings.installation_id
        print utils.json_dumps_pretty(version)
        return

    lbrynet_log = conf.settings.get_log_filename()
    log_support.configure_logging(lbrynet_log, not args.quiet, args.verbose)  # 日志相关
    log.debug('Final Settings: %s', conf.settings.get_current_settings_dict())

    try:
        log.debug('Checking for an existing lbrynet daemon instance')
        # 用于检查是否有lbrynet-daemon的服务开启
        JSONRPCProxy.from_url(conf.settings.get_api_connection_string()).status()
        log.info("lbrynet-daemon is already running")
        return
    except Exception:
        log.debug('No lbrynet instance found, continuing to start')

    log.info("Starting lbrynet-daemon from command line")

    # 检查是否能够连接到internet
    # (默认是以socket方式连接到lbry.io官网,可以改为国内网站,如baidu.com,如果成功则返回True)
    if test_internet_connection():
        analytics_manager = analytics.Manager.new_instance()  # 各种配置信息的初始化以及配置第三方的数据分析
        start_server_and_listen(args.useauth, analytics_manager)
        reactor.run()  # 事件循环管理器,单例reactor(异步回调也是事件触发)
    else:
        log.info("Not connected to internet, unable to start")

def update_settings_from_args(args):
    conf.settings.update({
        'use_auth_http': args.useauth,
        'wallet': args.wallet,
    }, data_types=(conf.TYPE_CLI,))

@defer.inlineCallbacks
def start_server_and_listen(use_auth, analytics_manager):
    """
    Args:
        use_auth: set to true to enable http authentication
        analytics_manager: to send analytics
    """
    analytics_manager.send_server_startup()
    daemon_server = DaemonServer(analytics_manager)
    try:
        # inlinecallbacks装饰器借助yield关键字来实现异步处理
        yield daemon_server.start(use_auth)
        analytics_manager.send_server_startup_success()
    except Exception as e:
        log.exception('Failed to start lbrynet-daemon')
        analytics_manager.send_server_startup_error(str(e))
        daemon_server.stop()


if __name__ == "__main__":
    start()
