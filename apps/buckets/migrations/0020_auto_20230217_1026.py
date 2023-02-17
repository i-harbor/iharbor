# Generated by Django 3.2.13 on 2023-02-17 02:26

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('buckets', '0019_auto_20211025_1737'),
    ]

    operations = [
        migrations.AlterField(
            model_name='archive',
            name='ceph_using',
            field=models.CharField(default=None, max_length=16, null=True, verbose_name='CEPH集群配置别名'),
        ),
        migrations.AlterField(
            model_name='archive',
            name='pool_name',
            field=models.CharField(default=None, max_length=32, null=True, verbose_name='PoolName'),
        ),
        migrations.AlterField(
            model_name='bucket',
            name='ceph_using',
            field=models.CharField(default=None, max_length=16, null=True, verbose_name='CEPH集群配置别名'),
        ),
        migrations.AlterField(
            model_name='bucket',
            name='pool_name',
            field=models.CharField(default=None, max_length=32, null=True, verbose_name='PoolName'),
        ),
    ]
