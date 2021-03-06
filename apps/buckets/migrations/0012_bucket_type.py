# Generated by Django 2.2.14 on 2020-07-27 00:42

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('buckets', '0011_auto_20200217_0920'),
    ]

    operations = [
        migrations.AddField(
            model_name='bucket',
            name='type',
            field=models.SmallIntegerField(choices=[(0, '普通'), (1, 'S3')], default=0, verbose_name='桶类型'),
        ),
        migrations.AlterField(
            model_name='apiusagedescription',
            name='desc_for',
            field=models.SmallIntegerField(choices=[(0, '原生API说明'), (1, 'FTP说明'), (2, 'S3兼容API说明')], default=0,
                                           verbose_name='关于什么的说明'),
        ),
    ]
