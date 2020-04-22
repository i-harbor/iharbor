;(function() {

    //API域名
    let DOMAIN_NAME = get_domain_url(); //'http://10.0.86.213:8000/';

    // 获取API域名
    function get_api_domain_name(){
        return DOMAIN_NAME;
    }

    // 构建带域名url
    function build_url_with_domain_name(url){
        let domain = get_api_domain_name();
        domain = domain.rightStrip('/');
        if(!url.startsWith('/'))
            url = '/' + url;
        return domain + url;
    }

    //
    // 分享base api
    function build_share_base_api(share_base, params=null){
        let api = '/share/list/' + share_base;
        if (params){
            api = api + "?" + encode_params(params);
        }
        return api;
    }

    //
    // 分享base url
    function build_share_base_url(share_base, params=null){
        return build_url_with_domain_name(build_share_base_api(share_base, params));
    }

    /**
     * 拼接数组为url字符串
     * @param {Array} arr - 待拼接的数组
     * @returns {string} - 拼接成的请求字符串
     */
    function encode_paths(arr) {
        const newArr = [];
        arr.forEach((value) => {
            if (value !== '')
                newArr.push(encodeURIComponent(value));
        }) ;

        return newArr.join('/');
    }

    //
    //所有ajax的请求的全局设置
    //
    $.ajaxSettings.beforeSend = function(xhr, settings){
        var csrftoken = getCookie('csrftoken');
        if (!csrfSafeMethod(settings.type) && !this.crossDomain) {
            xhr.setRequestHeader("X-CSRFToken", csrftoken);
        }
    };

    var SHARE_CODE = "";
    function get_share_code(){
        return SHARE_CODE;
    }
    //
    // 页面刷新时执行
    window.onload = function() {
        let sharebase = $("#id-share-base").attr("data-sharebase");
        SHARE_CODE = $("#id-share-base").attr("data-sharecode");
        let url = build_share_base_url(sharebase, {p:SHARE_CODE});
        get_bucket_files_and_render(url, render_bucket_files_view);
    };

    //网页内容显示区div
    $content_display_div = $("#content-display-div");

    /**
     * 路径字符串分割面包屑
     * @param path
     * @returns { [[], []] }
     */
    function get_breadcrumb(path) {
        let breadcrumb = [];
        if (path){
            path = path.strip('/');
            if (path !== '') {
                arr = path.split('/');
                for (var i = 0, j = arr.length; i < j; i++) {
                    breadcrumb.push([arr[i], arr.slice(0, i + 1).join('/')]);
                }
            }
        }
        return breadcrumb;
    }

    // 翻译字符串，包装django的gettext
    function getTransText(str){
        try{
            return gettext(str);
        }catch (e) {}
        return str;
    }
    function transInterpolate(fmt, obj, named){
        try {
            return interpolate(fmt, obj, named)
        }catch (e) {}

        if (named) {
            return fmt.replace(/%\(\w+\)s/g, function (match) {
                return String(obj[match.slice(2, -2)])
            });
        } else {
            return fmt.replace(/%s/g, function (match) {
                return String(obj.shift())
            });
        }
    }

    //art-template渲染模板注册过滤器
    template.defaults.imports.get_breadcrumb = get_breadcrumb;
    template.defaults.imports.sizeFormat = sizeFormat;
    template.defaults.imports.isoTimeToLocal = isoTimeToLocal;
    template.defaults.imports.getTransText = getTransText;
    template.defaults.imports.interpolate = transInterpolate;

    // 表格中每一行单选checkbox
    $("#content-display-div").on('click', '.item-checkbox', function () {
        if ($(this).prop('checked')){
            $(this).parents('tr').addClass('danger');
        }else{
            $(this).parents('tr').removeClass('danger');
        }
    });

    //
    //存储桶文件列表视图渲染模板
    //
    let render_bucket_files_view = template.compile(`
        <div class="container-fluid">
            <div class="row">
                <div class="col-sm-12">
                    <!--{#目录导航栏#}-->
                    <div>
                        <ol class="breadcrumb">
                            <li class="breadcrumb-item"><a href="" id="id-share-home" data-bucketname="{{ $data['bucket_name']}}"  data-sharebase="{{ $data['share_base']}}" data-subpath="{{ $data['subpath']}}"><i class="fa fa-home"></i></a></li>
                            <span>></span>
                            {{set breadcrumbs = $imports.get_breadcrumb($data['subpath'])}}
                            {{ each breadcrumbs }}
                                <li class="breadcrumb-item"><a href=""  id="btn-path-item" data-subpath="{{$value[1]}}">{{ $value[0] }}</a></li>
                            {{/each}}
                        </ol>
                    </div>
                </div>
            </div>
            <hr style=" height:1px;border:1px;border-top:1px solid #185598;"/>
            <div class="row">
                <div class="col-12">
                    <table class="table" id="bucket-files-table">
                        <tr class="bg-light">
                            <th><input type="checkbox" data-check-target=".item-checkbox" /></th>
                            <th>{{$imports.getTransText('名称')}}</th>
                            <th>{{$imports.getTransText('上传时间')}}</th>
                            <th>{{$imports.getTransText('大小')}}</th>
                            <th></th>
                        </tr>
                        {{set str_operation = $imports.getTransText('操作')}}
                        {{set str_open = $imports.getTransText('打开')}}
                        {{set str_download = $imports.getTransText('下载')}}
                        {{each files}}
                            <tr class="bucket-files-table-item">
                                <td><input type="checkbox" class="item-checkbox" value=""></td>
                                <!--文件-->
                                {{ if $value.fod }}
                                    <td class="bucket-files-table-item">
                                        <i class="fa fa-file"></i> {{ $value.name }}
                                    </td>
                                    <td>{{ $imports.isoTimeToLocal($value.ult) }}</td>
                                    <td>{{ $imports.sizeFormat($value.si, "B") }}</td>
                                {{/if}}
                                {{ if !$value.fod }}
                                    <td>
                                        <i class="fa fa-folder"></i>
                                        <a href="#" id="bucket-files-item-enter-dir" data-dirname="{{$value.name}}"><strong class="bucket-files-table-item" >{{ $value.name }}</strong></a>
                                    </td>
                                    <td>{{ $imports.isoTimeToLocal($value.ult) }}</td>
                                    <td>--</td>
                                {{/if}}
                                <td>
                                    <div class="dropdown">
                                        <button class="dropdown-toggle btn btn-outline-info" data-toggle="dropdown" role="button" aria-haspopup="true"
                                   aria-expanded="false">{{str_operation}}<span class="caret"></span></button>
                                        <ul class="dropdown-menu">
                                            <!--目录-->
                                            {{ if !$value.fod }}
                                                <a class="dropdown-item bg-info" href="" id="bucket-files-item-enter-dir" data-subpath="{{ $data['share_base']}}" data-dirname="{{$value.name}}">{{str_open}}</a>
                                            {{/if}}
                                            <!--文件-->
                                            {{ if $value.fod }}
                                                <a class="dropdown-item bg-success" id="bucket-files-item-download" href="{{$value.download_url}}" >{{str_download}}</a>
                                        {{/if}}
                                        </ul>
                                    </div>
                                </td>
                            </tr>
                        {{/each}}
                        <tr><td colspan="6"><%= $imports.interpolate($imports.getTransText('共 %s 个项目'), [count]) %></td></tr>
                    </table>
                </div>
            </div>
            {{if (previous || next)}}
            <div class="container-fluid">
                <div class="row">
                <nav aria-label="Page navigation col-6" style="margin:0;">
                  <ul class="pagination">
                    {{if previous}}
                        <li class="page-item"><a class="page-link" id="page_previous_bucket_files" href="{{previous}}"><span aria-hidden="true">&laquo;</span>{{$imports.getTransText('上页')}}</a></li>
                    {{/if}}
                    {{if !previous}}
                        <li class="page-item disabled"><a class="page-link"><span aria-hidden="true">&laquo;</span>{{$imports.getTransText('上页')}}</a></li>
                    {{/if}}                
                    {{if page}}
                        <li class="page-item disabled"><span class="page-link"><%= $imports.interpolate($imports.getTransText('第%s页 / 共%s页'), [page.current, page.final]) %></span></li>
                    {{/if}}
                    {{if next}}
                        <li class="page-item"><a class="page-link" id="page_next_bucket_files" href="{{next}}">{{$imports.getTransText('下页')}}<span aria-hidden="true">&raquo;</span></a></li>
                    {{/if}}
                    {{if !next}}
                        <li class="page-item disabled"><span class="page-link">{{$imports.getTransText('下页')}}<span aria-hidden="true">&raquo;</span></span></li>
                    {{/if}}
                  </ul>
                </nav>
                {{if page.final > 2}}
                    <div class="input-group mb-3 col-6">
                      <div class="input-group-prepend">
                        <span class="input-group-text">{{$imports.getTransText('跳转到')}}</span>
                      </div>
                      <input type="text" class="form-control" name="page-skip-to" style="max-width: 60px;">
                      <div class="input-group-append">
                        <span class="input-group-text">{{$imports.getTransText('页')}}</span>
                        <button class="btn btn-sm btn-primary" id="btn-skip-to-page" data-bucket_name="{{ $data['bucket_name'] }}" data-dir_path="{{ $data['dir_path'] }}">{{$imports.getTransText('跳转')}}</button>
                      </div>
                    </div>
                {{/if}}
                </div>
            </div>
            {{/if}}
        </div>
     `);

    // 获取分享基路径，子路径
    function get_share_base_and_sub() {
        let dom_home = $("#id-share-home");
        let ret = {};
        ret.sharebase = dom_home.attr("data-sharebase");
        ret.subpath = dom_home.attr("data-subpath");
        return ret;
    }

    //
    // 面包屑路径导航点击进入事件处理
    //
    $("#content-display-div").on("click", '#btn-path-item', function (e) {
        e.preventDefault();

        let r = get_share_base_and_sub();
        let subpath = $(this).attr("data-subpath");
        let url = build_share_base_url(r.sharebase, {subpath: subpath});
        get_bucket_files_and_render(url);
    });


    //
    // 文件夹、文件对象列表上一页Previous点击事件
    //
    $("#content-display-div").on("click", '#page_previous_bucket_files', function (e) {
        e.preventDefault();
        let url = $(this).attr('href');
        get_bucket_files_and_render(url);
    });

    //
    // 文件夹、文件对象列表下一页Next点击事件
    //
    $("#content-display-div").on("click", '#page_next_bucket_files', function (e) {
        e.preventDefault();
        let url = $(this).attr('href');
        get_bucket_files_and_render(url);
    });

    // 文件夹、文件对象列表 跳转到页码点击事件
    $("#content-display-div").on("click", '#btn-skip-to-page', function (e) {
        e.preventDefault();
        let page_num = $(":input[name='page-skip-to']").val();
        page_num = parseInt(page_num);
        if (isNaN(page_num) || page_num <= 0){
            show_auto_close_warning_dialog("请输入一个有效的正整数页码");
            return;
        }

        let r = get_share_base_and_sub();
        let limit = 200;
        let offset = (page_num - 1) * limit;
        let sc = get_share_code();
        let url = build_share_base_url(r.sharebase, {subpath: r.subpath, offset: offset, limit: limit, p: sc});
        get_bucket_files_and_render(url);
    });

    //
    // 存储桶列表文件夹点击进入事件处理
    //
    $("#content-display-div").on("click", '#bucket-files-item-enter-dir', function (e) {
        e.preventDefault();
        let dirname = $(this).attr("data-dirname");
        let r = get_share_base_and_sub();
        let path = dirname;
        if (r.subpath){
            path = r.subpath + '/' + dirname;
        }
        let sc = get_share_code();
        let url = build_share_base_url(r.sharebase, {subpath: path, p: sc});
        get_bucket_files_and_render(url);
    });

    //
    // 获取存储桶文件列表并渲染
    //
    function get_bucket_files_and_render(url){
        get_content_and_render(url, render_bucket_files_view);
    }

    //
    // GET请求数据并渲染接口封装
    //
    function get_content_and_render(url, render, data={}){
        swal.showLoading();
        $.ajax({
            url: url,
            data: data,
            timeout: 20000,
            success: function(data,status,xhr){
                swal.close();
                if (status === 'success'){
                    let html = render(data);
                    $content_display_div.empty();
                    $content_display_div.append(html);
                }else{
                    show_warning_dialog('好像出问题了，跑丢了，( T__T ) …', 'error');
                }
            },
            error: function (xhr, errtype, error) {
                swal.close();
                if (errtype === 'timeout'){
                    show_warning_dialog('请求超时', 'error');
                }else{
                    show_warning_dialog('好像出问题了，跑丢了，( T__T ) …', 'error');
                }
            }
        });
    }

})();
