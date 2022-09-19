# -*- coding: utf-8 -*-
# Generated by Django 1.11.18 on 2019-01-28 05:57
from __future__ import unicode_literals

import ckeditor.fields
from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):



    initial = True

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='Bucket',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(db_index=True, max_length=63, verbose_name='bucket名称')),
                ('created_time', models.DateTimeField(auto_now_add=True, verbose_name='创建时间')),
                ('collection_name', models.CharField(default='', max_length=50, verbose_name='存储桶对应的表名')),
                ('user', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to=settings.AUTH_USER_MODEL, verbose_name='所属用户')),
                ('access_permission', models.SmallIntegerField(choices=[(1, '公有'), (2, '私有')], default=2, verbose_name='访问权限')),
                ('soft_delete', models.BooleanField(choices=[(True, '删除'), (False, '正常')], default=False, verbose_name='软删除')),
                ('modified_time', models.DateTimeField(auto_now=True, verbose_name='修改时间')),
                ('objs_count', models.IntegerField(default=0, verbose_name='对象数量')),
                ('size', models.BigIntegerField(default=0, verbose_name='桶大小')),
            ],
            options={
                'verbose_name': '存储桶',
                'verbose_name_plural': '存储桶',
                'ordering': ['-created_time'],
            },
        ),
        migrations.CreateModel(
            name='BucketLimitConfig',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('limit', models.IntegerField(default=0, verbose_name='可拥有存储桶上限')),
                ('user', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, related_name='bucketlimit', to=settings.AUTH_USER_MODEL, verbose_name='用户')),
            ],
            options={
                'verbose_name': '桶上限配置',
                'verbose_name_plural': '桶上限配置',
            },
        ),
        migrations.CreateModel(
            name='ApiUsageDescription',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('title', models.CharField(default='使用说明', max_length=255, verbose_name='标题')),
                ('content', ckeditor.fields.RichTextField(default='', verbose_name='说明内容')),
                ('created_time', models.DateTimeField(auto_now_add=True, verbose_name='创建时间')),
                ('modified_time', models.DateTimeField(auto_now=True, verbose_name='修改时间')),
            ],
            options={
                'verbose_name': '使用说明',
                'verbose_name_plural': '使用说明',
            },
        ),
    ]
