# Generated by Django 3.2.4 on 2021-10-25 09:37

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('buckets', '0018_auto_20211020_1600'),
    ]

    operations = [
        migrations.AddField(
            model_name='backupbucket',
            name='error',
            field=models.CharField(blank=True, default='', max_length=255, verbose_name='错误信息'),
        ),
        migrations.AlterField(
            model_name='backupbucket',
            name='bucket_token',
            field=models.CharField(max_length=64, verbose_name='备份点bucket读写token'),
        ),
    ]
