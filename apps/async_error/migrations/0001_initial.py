# Generated by Django 3.2.13 on 2023-03-23 07:50

import django.core.validators
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='BucketAsyncError',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('node_ip', models.CharField(max_length=255, verbose_name='节点ip地址')),
                ('bucket_id', models.IntegerField(validators=[django.core.validators.MinValueValidator(1)], verbose_name='存储桶id')),
                ('bucket_name', models.CharField(max_length=63, verbose_name='存储桶名称')),
                ('object_id', models.IntegerField(validators=[django.core.validators.MinValueValidator(1)], verbose_name='对象id')),
                ('object_name', models.CharField(max_length=255, verbose_name='对象名称')),
                ('async_error', models.TextField(verbose_name='同步错误')),
                ('error_time', models.DateTimeField(auto_now=True, verbose_name='时间')),
                ('backup_ip', models.CharField(max_length=255, verbose_name='备份地址')),
                ('backup_bucket', models.CharField(max_length=63, verbose_name='备份存储桶名称')),
                ('node_num', models.IntegerField(validators=[django.core.validators.MinValueValidator(1)], verbose_name='节点编号')),
                ('node_count', models.IntegerField(validators=[django.core.validators.MinValueValidator(1)], verbose_name='节点总数')),
                ('thread_num', models.IntegerField(blank=True, null=True, validators=[django.core.validators.MinValueValidator(0)], verbose_name='并发线程数')),
                ('bucketlist', models.CharField(blank=True, max_length=255, null=True, verbose_name='指定备份的桶')),
            ],
            options={
                'verbose_name': '数据同步错误日志',
                'verbose_name_plural': '数据同步错误日志',
                'ordering': ('id',),
            },
        ),
    ]
