from django.core.management.commands import makemessages


class Command(makemessages.Command):
    """
    自定义生成翻译文件的命令，通过--extra-keyword指定其他翻译函数名关键字
    """
    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            '--extra-keyword',
            dest='extra_keywords',
            action='append',
            help='指定其他翻译函数名关键字，查找需要翻译的内容'
        )

    def handle(self, *args, **options):
        keywords = options.pop('extra_keywords')
        if keywords:
            self.xgettext_options = (
                makemessages.Command.xgettext_options[:] +
                ['--keyword=%s' % kwd for kwd in keywords]
            )
        super().handle(*args, **options)
