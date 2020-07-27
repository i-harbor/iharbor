提供部分兼容AWS S3接口的api访问iHarbor服务。

#### 注意事项
1. 兼容AWS S3接口和iHarbor原生api和网页端删除和上传对象不可混用；
（a）兼容AWS S3接口的api使用iHarbor服务创建的桶bucket，不可通过iHarbor原生api和网页端访问（删除、上传）对象，
多部分上传对象part元数据会产生脏数据，会影响S3接口的使用；
（b）通过iHarbor原生api和网页端创建的桶bucket，没有S3对象part元数据信息和概念,无法使用兼容AWS S3接口访问（上传、下载、删除、HeadObject等）桶内的对象；

#### 域名endpoint  
只支持virtual-hosted style URL访问bucket的方式，不支持path style URL访问bucket的方式，无region。    
endpoint格式：http://<BUCKET>.s3.obs.cstcloud.cn

#### 存储类型
对象存储iHarbor只支持标准（STANDARD）一种存储类型, 默认为STANDARD。

#### 签名认证
兼容AWS Signature Version 4，支持请求头签名和参数签名方式。

#### 公共标头
| 标头 | 说明 |
| :-----| :---- |
| Authorization | 兼容 |
| Content-Length | 兼容 |
| Content-Type | 兼容 |
| Content-MD5 | 兼容 |
| Date | 兼容 |
| Host | 兼容 |
| ETag | 兼容 |
| x-amz-request-id | 不支持 |
| x-amz-id-2 | 不支持 |
| x-amz-version-id | 不支持 |

#### 兼容API
##### Service API
| S3 API | 请求 | 响应 |
| :-----| :---- | :----: |
| ListBuckets | 兼容 | 兼容 |

##### Bucket API
| S3 API | 请求 | 响应 |
| :-----| :---- | :----: |
| CreateBucket | 不支持区域指定，不需要请求体；<br>参数x-amz-acl部分支持，有效值: private，public-read，public-read-write；其他参数不支持； | 兼容 |
| DeleteBucket | 兼容 | 兼容 |
| HeadBucket | 兼容 | 兼容 |

##### Object APi
| S3 API | 请求 | 响应 | 说明 |
| :----| :---- | :---- | :---- |
| PutObject | 支持请求头:<br>x-amz-storage-class只能取值STANDARD，默认STANDARD；<br>x-amz-acl部分支持，有效值: private，public-read，public-read-write；其他参数不支持；| 仅兼容ETag |只支持基础上传功能，目录创建 |
| DeleteObject | VersionId参数不支持，标头x-amz-*不支持； | 标头x-amz-*不支持； |只支持基础删除功能，删除对象或目录，此请求会直接物理永久删除对象|
| ListObjectV2 | 兼容 | 兼容 |无|
| GetObject | 支持标头：Range, If-*；<br>仅支持参数：partNumber，response-content-disposition，response-content-type，<br>response-content-encoding， response-content-language；| 兼容标头：Last-Modified, Content-Length, ETag, Content-Disposition, Content-Encoding，Content-Language，Content-Range，Content-Type； |无|
| HeadObject | 支持标头：Range, If-*；<br>支持参数：partNumber； | 兼容标头：Last-Modified, Content-Length, ETag, Content-Disposition, Content-Encoding，Content-Language，Content-Range，Content-Type； |无|
| CreateMultipartUpload | 标头x-amz-acl部分支持，有效值: private，public-read，public-read-write；<br>标头x-amz-storage-class默认并只能为STANDARD, 其他参数不支持； | 标头x-amz-*不兼容 | 仅支持基础功能 |
| AbortMultipartUpload | 标头x-amz-request-payer不支持； | 标头x-amz-request-payer不支持； | 无 |
| CompleteMultipartUpload | 标头参数不支持； | 标头x-amz-*不兼容 | 仅支持基础功能；part越多，大小越大，合并一个对象需要的时间越长;<br>如果已请求合并未响应之前，重试发起第二次请求，会得到一个CompleteMultipartAlreadyInProgress错误码； |
| UploadPart | 标头x-amz-*不支持 | 标头x-amz-*不支持 | 仅支持基础上传功能 |




