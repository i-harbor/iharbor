import os
import re
import shutil

from django.conf import settings
from django.db import models
from django.utils.translation import gettext_lazy as _
from django.core.exceptions import ValidationError

from utils.oss import HarborObject


class CephCluster(models.Model):
    """
    ceph 集群配置信息
    """
    id = models.AutoField(primary_key=True)
    name = models.CharField(verbose_name='集群名称', max_length=50, unique=True, blank=False)
    cluster_name = models.CharField(verbose_name='CLUSTER_NAME配置名称', max_length=50, blank=False)
    user_name = models.CharField(verbose_name='用户名称(USER_NAME)', max_length=50, blank=False)
    disable_choice = models.BooleanField(verbose_name='禁用选择(DISABLE_CHOICE)', default=False,
                                         help_text="默认false, 如果为true则该ceph配置不启用")
    pool_names = models.JSONField(verbose_name='POOL存储池名称', blank=False, default=list,
                                  help_text="配置标准：[\"池名1\",\"池名2\" ...]")

    config = models.TextField(verbose_name='ceph集群配置文本', default='')
    config_file = models.CharField(max_length=200, editable=False, blank=True, verbose_name='配置文件保存路径',
                                   help_text="点击保存，配置文本会存储到这个文件, 此字段自动填充")
    keyring = models.TextField(verbose_name='ceph集群keyring文本')
    keyring_file = models.CharField(max_length=200, editable=False, blank=True, verbose_name='keyring文件保存路径',
                                    help_text="点击保存，keyring文本会存储到这个文件, 此字段自动填充")
    modified_time = models.DateTimeField(auto_now=True, verbose_name='修改时间')
    alias = models.CharField(verbose_name='CEPH集群配置别名', max_length=16, blank=False, default='default', unique=True)
    remarks = models.CharField(verbose_name='备注', max_length=255, blank=True, default='')

    class Meta:
        ordering = ('id',)
        verbose_name = 'CEPH集群'
        verbose_name_plural = 'CEPH集群'

    def __str__(self):
        return self.name

    def get_config_file(self):
        """
        ceph配置文件路径
        :return: str
        """
        if not self.config_file:
            self.save_config_to_file()

        return self.config_file

    def get_keyring_file(self):
        """
        ceph keyring文件路径
        :return: str
        """
        if not self.keyring_file:
            self.save_config_to_file()

        return self.keyring_file

    def save_config_to_file(self, path=None):
        """
        ceph的配置内容保存到配置文件

        :return:
            True    # success
            False   # failed
        """
        if not path:
            path = os.path.join(settings.BASE_DIR, 'data/ceph/conf/')
        else:
            path = os.path.join(settings.BASE_DIR, path)
        self.config_file = os.path.join(path, f'{self.alias}.conf')
        self.keyring_file = os.path.join(path, f'{self.alias}.keyring')

        try:
            # 目录路径不存在存在则创建
            os.makedirs(path, exist_ok=True)

            with open(self.config_file, 'w') as f:
                config = self.config.replace('\r\n', '\n')  # Windows
                self.config = config.replace('\r', '\n')  # MacOS
                f.write(self.config + '\n')  # 最后留空行

            with open(self.keyring_file, 'w') as f:
                keyring = self.keyring.replace('\r\n', '\n')
                self.keyring = keyring.replace('\r', '\n')
                f.write(self.keyring + '\n')
        except Exception:
            return False

        return True

    def save(self, *args, **kwargs):
        self.save_config_to_file()
        super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        # 删除配置文件
        os.remove(self.config_file)
        os.remove(self.keyring_file)
        super().delete(*args, **kwargs)

    def clean(self):
        """
        校验模型中字段内容
        """
        if not isinstance(self.pool_names, list):
            raise ValidationError({'pool_names': _('字段类型必须是一个json格式的数组。')})

        if not self.pool_names:
            raise ValidationError({'pool_names': _('该字段不不能为空。')})

        if not all(self.pool_names):
            raise ValidationError({'pool_names': _('存储池不能有为空的情况。')})

        # 备份路径用于测试ceph连接
        path = f'data/ceph/{self.alias}test/conf'
        self.save_config_to_file(path=path)

        ho = HarborObject(pool_name='', obj_id='', obj_size=2, cluster_name=self.cluster_name,
                          user_name=self.user_name, conf_file=self.config_file, keyring_file=self.keyring_file)
        try:
            list_pool_cluster = ho.rados.get_cluster().list_pools()
            for pool_name in self.pool_names:
                if pool_name not in list_pool_cluster:
                    raise ValidationError({'pool_names': _(f'集群中该存储池 {pool_name} 不存在。')})
        except ValidationError as exc:
            raise exc
        except Exception as e:
            # [errno 22] RADOS invalid argument (error calling conf_read_file)
            # [errno 22] RADOS invalid argument (error calling conf_read_file)
            # [errno 5] RADOS I/O error (error connecting to the cluster)
            # [errno 1] RADOS permission error (error connecting to the cluster) user_name
            # [errno 13] RADOS permission denied (error connecting to the cluster) user_name
            errno = getattr(e, 'errno', None)
            if not errno:
                raise ValidationError({_('无法连接ceph集群，请重新查看填写的配置。')})
            elif errno == 22:
                raise ValidationError({'config': _('配置文件填写有误： RADOS invalid argument (error calling '
                                                   'conf_read_file)。'),
                                       'keyring': _('配置文件填写有误：RADOS invalid argument (error calling '
                                                    'conf_read_file)。')})
            elif errno == 1 or errno == 13:
                raise ValidationError({'user_name': _('该字段填写有误：RADOS permission error/denied (error connecting to '
                                                      'the cluster)。')})
            output = _(f'请查看ceph集群是否正常使用，报错：{e}。')
            raise ValidationError(output)
        finally:
            # 删除测试的备份路径
            shutil.rmtree(f'data/ceph/{self.alias}test')
