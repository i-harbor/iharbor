import json

from django.core.management.base import BaseCommand, CommandError
from rest_framework import serializers

from buckets.models import Bucket


class BucketSerializer(serializers.Serializer):
    """
    存储桶序列化器
    """
    id = serializers.IntegerField()
    name = serializers.CharField()
    created_time = serializers.DateTimeField()
    user = serializers.SerializerMethodField()  # 自定义user字段内容

    @staticmethod
    def get_user(obj):
        return {'id': obj.user.id, 'username': obj.user.username}


class Command(BaseCommand):
    help = """
        export buckets to file '/home/export-bucket.txt':
        [manage.py exportbucket --filename="/home/export-bucket.txt"];
    """

    def add_arguments(self, parser):
        parser.add_argument(
            '--filename', default=None, dest='filename', type=str,
            help='The file that export buckets to.',
        )

    def handle(self, *args, **options):
        filename = options.get('filename')
        if not filename:
            filename = '/home/export-bucket.txt'

        self.stdout.write(self.style.WARNING(f'Export bucket to file: {filename}'))
        buckets = self.get_buckets()
        self.stdout.write(self.style.NOTICE(f'Count of buckets: {buckets.count()}'))
        if input('Are you sure you want to do this?\n\n' + "Type 'yes' to continue, or 'no' to cancel: ") != 'yes':
            raise CommandError("cancelled.")

        self.export_buckets(buckets, filename=filename)

    @staticmethod
    def get_buckets():
        buckets = Bucket.objects.select_related('user').all()
        buckets = buckets.order_by('id')
        return buckets

    def export_buckets(self, buckets, filename: str):
        with open(filename, 'w+') as f:
            for b in buckets:
                line = self.build_line_str(b)
                f.write(line)

        self.stdout.write(self.style.SUCCESS(f'Successfully export {len(buckets)} buckets to file: {filename}'))

    @staticmethod
    def build_line_str(b):
        slz = BucketSerializer(instance=b)
        line = json.dumps(slz.data)
        return line + '\n'
