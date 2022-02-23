
安全凭证是用于用户身份认证的凭证，iHarbor云对象存储服务提供了多种安全认证方式，如Session,Token,JWT,访问密钥。

## Auth Token认证 
Token密钥认证方式，使用简单，安全性相对较低，推荐内网使用，token的获取可以通过开放的API获取,或者去iHarbor站点通过浏览器端网页获取。
每个用户同一时刻只有一个有效的token，token永久有效，没有有效期，刷新创建新token，旧token会失效，如果token泄露，请及时创建新的token，以防数据泄露丢失。    

Token应包含在Authorization HTTP标头中，密钥应以字符串文字“Token”为前缀，空格分隔两个字符串。
例如：   
`Authorization: Token 9944b09199c62bcf9418ad846dd0e4bbdfc6ee4b`  

## JWT认证
Json web token认证方式，使用简单，包括access jwt和refresh jwt，access jwt有效期为8h，旧的access jwt失效前可以通过对应API携
带旧refresh jwt在有效时限（2天）内刷新获取新的access jwt。
jwt应包含在Authorization HTTP标头中，密钥应以字符串文字“Bearer”为前缀，空格分隔两个字符串，例如：   
`Authorization: Bearer eyJhbGciOiAiSFMyNTYiLCAidHlwIj`

## 科技云通行证JWT认证
支持科技云通行证JWT认证，jwt应包含在Authorization HTTP标头中，密钥应以字符串文字“AAI-JWT”为前缀，空格分隔两个字符串，例如：   
`Authorization: AAI-JWT eyJhbGciOiAiSFMyNTYiLCAidHlwIj`

## 访问密钥  
访问密钥是一个密钥对（AccessKey和SecretKey），AccessKey会在网络中传输，SecretKey不在网络上传输，需要用户妥善保管。
密钥对用于安全凭证的生成，通过一些签名算法，以SecretKey为加密参数，对一些适当的数据内容进行加密生成安全凭证。访问密钥认证
方式安全性高，使用会复杂一些。  
若SecretKey意外泄露或被恶意第三方窃取，可能导致数据泄漏风险。若发生密钥泄露等安全问题，密钥拥有着应第一时间在iHarbor平台
的安全凭证中更换密钥。

### 访问密钥凭证格式和用法  
安全凭证auth_key的格式为`evhb-auth {access_key}:{hmac_sha1}:{data_base64}`，包含在HTTP标头Authorization中，
凭证应以字符串文字“evhb-auth”为前缀，空格分隔两个字符串。  
例如：`Authorization: evhb-auth xxx:xxx:xxx`  

### 访问密钥凭证生成  

安全凭证是客户端请求内容的一部分，不带凭证或带非法凭证的请求将返回HTTP错误码401，代表认证失败。

生成安全凭证时需要指定以下要素：  
 >* __请求的url的全路径非编码path__，不包含域名，如url为`“http://abc.com/a/d?b=1”`，取path为`“/a/d?b=1”`；   
 >* __请求方法method__, 如GET,POST,PUT,PATCH,DELETE等；   
 >* __有效期时间戳__；  

以下python为例，说明生成安全凭证的过程，所需数据如下： 
>* path_of_url = '/a/d?b=1';   
>* method = 'GET'；   
>* deadline = 1551251800;  
>* access_key = '4203ecc034d411e9b31bc800a000655d',
>* secret_key = '93c74b39396abd09cb0720a1af52c5c27690a2b8'    

在python中，数据data以字典格式组织:  
`{ 'path_of_url': '/a/d?b=1', 'method': 'GET', 'deadline': 1551253771 }`；

1、 首先data要序列化为json格式的字符串data_json：   
`'{"path_of_url":"/a/d?b=1","method":"GET","deadline":1551253771}'`   
 
2、 接着data_json字符串以utf-8编码为bytes,再通过base64编码，以utf-8解码为字符串data_base64:   
`'eyJwYXRoX29mX3VybCI6Ii9hL2Q_Yj0xIiwibWV0aG9kIjoiR0VUIiwiZGVhZGxpbmUiOjE1NTEyNTM3NzF9'`  

3、 然后以secret_key为密钥参数，对data_base64字符串utf8编码后的bytes做HMAC-SHA1哈希签名，得到sha1哈希bytes如下：   
`b'A\xb0g\xd6\x99\xc8\xa2\xc1DfH\n\xcdP\x1e\xfa\xe6\xca\xee\xb8'`  

4、 然后再对sha1哈希签名bytes做base64编码，以uft8解码为字符串hmac_sha1如下:  
`'QbBn1pnIosFEZkgKzVAe-ubK7rg='`   

5、 安全凭证auth_key的格式为“evhb-auth {access_key}:{hmac_sha1}:{data_base64}”,最后按格式拼接各字符串得到安全凭证auth_key如下：  
`'evhb-auth 4203ecc034d411e9b31bc800a000655d:QbBn1pnIosFEZkgKzVAe-ubK7rg=:eyJwYXRoX29mX3VybCI6Ii9hL2Q_Yj0xIiwibWV0aG9kIjoiR0VUIiwiZGVhZGxpbmUiOjE1NTEyNTM3NzF9'`   

具体请参考: [Python代码](https://github.com/i-harbor/webserver/blob/master/apps/users/auth/auth_key.py)，[Golang代码](https://github.com/i-harbor/goharbor/blob/master/authkey.go)    
**注意**：由于安全凭证auth_key有时间戳deadline授权截止时间，客户端和服务器需要同步校准各自的时钟。

## 资源访问权限

存储桶下资源的访问权限分2级权限，存储桶访问权限和桶下资源自己的访问权限；非存储桶所有者对资源的可见性，存储桶访问权限优先于存储桶下资源自己的访问权限：
* 存储桶权限是公有的，桶下所有资源都是公有权限；  
* 存储桶私有权限时，桶下资源自己的访问权限才有效；


### 存储桶访问权限
存储桶的所有者拥有存储桶的所有权限和管理权限，存储桶权限包含“私有”、“公有只读”、“公有可读可写”。
* 私有： 非存储桶所有者不可以访问桶下资源，桶下分享公开的资源除外；
* 公有只读：非存储桶所有者只可读取下载桶下资源；
* 公有可读可写：非存储桶所有者可上传可下载对象，创建和删除目录和对象；

### 存储桶下资源单独访问权限
存储桶下资源的单独访问权限包含“私有”、“公有只读”、“公有可读可写”，存储桶私有权限时才有效。

### 公开和分享
带密码的分享，同时只用一个分享密码有效，创建新的带密码的分享，旧的分享密码会失效。














