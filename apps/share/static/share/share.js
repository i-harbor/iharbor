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

    //
    // 页面刷新时执行
    window.onload = function() {
        let sharebase = $("#id-share-base").attr("data-sharebase");
        let url = build_share_base_url(sharebase);
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

    //art-template渲染模板注册过滤器
    template.defaults.imports.get_breadcrumb = get_breadcrumb;
    template.defaults.imports.sizeFormat = sizeFormat;

    //
    // 表格中每一行单选checkbox
    //
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
                <div class="col-xs-12 col-sm-12">
                    <!--{#目录导航栏#}-->
                    <div>
                        <ol class="breadcrumb">
                            <li><a href="" id="id-share-home" data-bucketname="{{ $data['bucket_name']}}"  data-sharebase="{{ $data['share_base']}}" data-subpath="{{ $data['subpath']}}"><span class="glyphicon glyphicon-home"></a></li>
                            <span>></span>
                            {{set breadcrumbs = $imports.get_breadcrumb($data['subpath'])}}
                            {{ each breadcrumbs }}
                                <li><a href=""  id="btn-path-item" data-subpath="{{$value[1]}}">{{ $value[0] }}</a></li>
                            {{/each}}
                        </ol>
                    </div>
                </div>
            </div>
            <hr style=" height:1px;border:1px;border-top:1px solid #185598;"/>
            <div class="row">
                <div class="col-xs-12 col-sm-12">
                    <table class="table table-responsive" id="bucket-files-table">
                        <tr class="bg-info">
                            <th><input type="checkbox" data-check-target=".item-checkbox" /></th>
                            <th>名称</th>
                            <th>上传时间</th>
                            <th>大小</th>
                            <th></th>
                        </tr>
                        {{each files}}
                            <tr class="bucket-files-table-item">
                                <td><input type="checkbox" class="item-checkbox" value=""></td>
                                <!--文件-->
                                {{ if $value.fod }}
                                    <td class="bucket-files-table-item">
                                        <span class="glyphicon glyphicon-file"></span>{{ $value.name }}
                                    </td>
                                    <td>{{ $value.ult }}</td>
                                    <td>{{ $imports.sizeFormat($value.si, "B") }}</td>
                                {{/if}}
                                {{ if !$value.fod }}
                                    <td>
                                        <span class="glyphicon glyphicon-folder-open"></span>
                                        <a href="#" id="bucket-files-item-enter-dir" data-dirname="{{$value.name}}"><strong class="bucket-files-table-item" >{{ $value.name }}</strong></a>
                                    </td>
                                    <td>{{ $value.ult }}</td>
                                    <td>--</td>
                                {{/if}}
                                <td>
                                    <li class="dropdown btn">
                                        <a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-haspopup="true"
                                   aria-expanded="false">操作<span class="caret"></span></a>
                                        <ul class="dropdown-menu">
                                            <!--目录-->
                                            {{ if !$value.fod }}
                                                <li class="btn-info"><a href="" id="bucket-files-item-enter-dir" data-subpath="{{ $data['share_base']}}" data-dirname="{{$value.name}}">打开</a></li>
                                            {{/if}}
                                            <!--文件-->
                                            {{ if $value.fod }}
                                                <li class="btn-success"><a id="bucket-files-item-download" href="{{$value.download_url}}" >下载</a></li>
                                        {{/if}}
                                        </ul>
                                    </li>
                                </td>
                            </tr>
                        {{/each}}
                    </table>
                    {{if files}}
                        {{if files.length === 0}}
                              <p class="text-info text-center">肚子空空如也哦 =^_^=</p>
                         {{/if}}
                     {{/if}}
                </div>
            </div>
            
            {{if (previous || next)}}
            <div class="row">
                <div class="col-xs-12 col-sm-12">
                   <nav aria-label="...">
                      <ul class="pager">
                        {{if previous}}
                            <li><a id="page_previous_bucket_files" href="{{previous}}"><span aria-hidden="true">&larr;</span>上页</a></li>
                        {{/if}}
                        {{if !previous}}
                            <li class="disabled"><a><span aria-hidden="true">&larr;</span>上页</a></li>
                        {{/if}}
                        
                        {{if page}}
                            <li>第{{page.current}}页 共{{page.final}}页</li>
                        {{/if}}
                        
                        {{if next}}
                            <li><a id="page_next_bucket_files" href="{{next}}">下页<span aria-hidden="true">&rarr;</span></a></li>
                        {{/if}}
                        {{if !next}}
                            <li class="disabled"><a>下页<span aria-hidden="true">&rarr;</span></a></li>
                        {{/if}}
                      </ul>
                    </nav>
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
    $("#content-display-div").on("click", '.pager #page_previous_bucket_files', function (e) {
        e.preventDefault();
        url = $(this).attr('href');
        get_bucket_files_and_render(url);
    });

    //
    // 文件夹、文件对象列表下一页Next点击事件
    //
    $("#content-display-div").on("click", '.pager #page_next_bucket_files', function (e) {
        e.preventDefault();
        url = $(this).attr('href');
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
        let url = build_share_base_url(r.sharebase, {subpath: path});
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
            error: function (error) {
                swal.close();
                show_warning_dialog('好像出问题了，跑丢了，( T__T ) …', 'error');
            }
        });
    }

})();
