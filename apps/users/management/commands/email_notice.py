import threading
from django.core.management.base import BaseCommand, CommandError
from django.core.mail import send_mass_mail
from django.contrib.auth import get_user_model
from django.conf import settings

User = get_user_model()

class Command(BaseCommand):
    '''
    为bucket对应的表，清理满足彻底删除条件的对象和目录
    '''
    pool_sem = threading.Semaphore(20)  # 定义最多同时启用多少个线程

    help = """** manage.py email_notice --all-user --msg="2019年10月22日18:00-19:00，iHarbor对象存储服务将维护更新，特此通知，给您带来的不便请谅解" """

    def add_arguments(self, parser):
        parser.add_argument(
            '--username', default=None, dest='username',
            help='the user that will send email to',
        )
        parser.add_argument(
            '--all-user', default=None, nargs='?', dest='all', const=True, # 当命令行有此参数时取值const, 否则取值default
            help='will send email to all user',
        )
        parser.add_argument(
            '--msg', default='', dest='msg', type=str,
            help='email content',
        )
        parser.add_argument(
            '--title', default='', dest='title', type=str,
            help='email title',
        )

    def handle(self, *args, **options):

        msg = options['msg']   # sql模板
        if not msg:
            raise CommandError("email must have some content")
        self.email_msg = msg

        title = options['title']
        self.email_title = title if title else 'iHarbor服务维护通知'

        users = self.get_users(**options)
        if input('Are you sure you want to do this?\n\n' + "Type 'yes' to continue, or 'no' to cancel: ") != 'yes':
            raise CommandError("cancelled.")

        self.send_email_to_users(users)

    def get_users(self, **options):
        '''
        获取给定的user或所有user
        :param options:
        :return:
        '''
        username = options['username']
        all = options['all']

        # 指定名字的用户
        if username:
            self.stdout.write(self.style.NOTICE('Will send email to user named {0}'.format(username)))
            return User.objects.value.filter(name=username).values_list('username').all()

        # 全部的桶
        if all is not None:
            self.stdout.write(self.style.NOTICE( 'Will send email to all user.'))
            return User.objects.values_list('username').all()

        raise CommandError("please give a username or all users")

    def send_emails_task(self, addrs:list):
        try:
            to = []
            for addr in addrs:
                if isinstance(addr, tuple):
                    addr = addr[0]
                to.append((self.email_title, self.email_msg, settings.EMAIL_HOST_USER, [addr]))

            c = len(to)
            s = send_mass_mail(datatuple=to, fail_silently=True)
            if s >= c:
                self.stdout.write(self.style.SUCCESS('Successfully send email to {0} user'.format(s)))
            else:
                self.stdout.write(self.style.NOTICE('Successfully send email to {0} user, failed {1}'.format(s, c-s)))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'send_emails_task error: {str(e)}'))

        self.pool_sem.release()  # 可用线程数+1

    def send_email_to_users(self, users):
        for addrs in self.generator_wrapper(users):
            if self.pool_sem.acquire(): # 可用线程数-1，控制线程数量，当前正在运行线程数量达到上限会阻塞等待
                worker = threading.Thread(target=self.send_emails_task, kwargs={'addrs': addrs})
                worker.start()

        # 等待所有线程结束
        while True:
            c = threading.active_count()
            if c <= 1:
                break

        self.stdout.write(self.style.SUCCESS('Successfully send email'))

    def generator_wrapper(self, users, num_per=100):
        '''
        包装生成器，每次从users中取出num_per个元素
        :param users: 生成器的源数据,需可被切片
        :param num_per: 每次返回的元素数量
        :return: users的切片
        '''
        start = 0
        l = len(users)
        while True:
            end = min(start + num_per, l)
            yield users[start: end]

            if end == l:
                break
            start = end
