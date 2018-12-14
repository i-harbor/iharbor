;(function() {

    //API域名
    let DOMAIN_NAME = ''; //'http://10.0.86.213:8000/';

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
    // 文件对象base api
    //
    function get_obj_base_api(){
        return 'api/v1/obj/';
    }

    //
    //构建对象api
    //错误返回空字符串
    function build_obj_detail_api(params={bucket_name: '', dir_path: '', filename: ''}){
        if (!params.hasOwnProperty('bucket_name') || !params.hasOwnProperty('dir_path') || !params.hasOwnProperty('filename'))
            return '';

        let bucket_path = get_cur_path_startwith_bucket({
            bucket_name: params.bucket_name,
            dir_path: params.dir_path
        });

        if (!bucket_path)
            return '';

        return get_obj_base_api() + bucket_path + params.filename + '/';
    }

    //
    //构建对象info api
    //错误返回空字符串
    function build_obj_info_api(params={bucket_name: '', dir_path: '', filename: ''}){
        let detail_api = build_obj_detail_api(params);
        if (!detail_api)
            return '';
        return detail_api + '?info=true';
    }

    //
    //构造文件对象上传url
    function build_obj_detail_url(params={bucket_name:'', dir_path: '', filename:''}) {
        let api = build_obj_detail_api(params);
        return build_url_with_domain_name(api);
    }


    //
    // 构造文件对象共享url
    //@ param share:是否公开，true or false
    //@ param days:公开时间天数，<0(不公开); 0(永久公开);
    // @returns url(string)
    //
    function build_obj_share_url(params={detail_url:'', share: '', days:''}) {
        return params.detail_url + '?share=' + params.share + '&days=' + params.days;

    }

    //
    // 目录base api
    //
    function get_dir_base_api(){
        return 'api/v1/dir/';
    }

    //构建目录detail api
    function build_dir_detail_api(params={bucket_name: '', dir_path: ''}){
        return get_dir_base_api() + get_cur_path_startwith_bucket(params);
    }

    //构建目录detail url
    function build_dir_detail_url(params={bucket_name: '', dir_path: ''}){
        let api = build_dir_detail_api(params);
        return build_url_with_domain_name(api);
    }

    //构建目录url
    function build_dir_url(path_with_bucket){
        let api = get_dir_base_api() + path_with_bucket;
        return build_url_with_domain_name(api);
    }

    //
    // 存储桶base api
    //
    function get_buckets_base_api(){
        return '/api/v1/buckets/';
    }

    //
    // 存储桶detail api
    //@param id:存储桶id, type:int
    function build_buckets_detail_api(id){
        return get_buckets_base_api() + id + '/';
    }

    //
    // 存储桶权限设置url
    //@param id:存储桶id, type:int
    //@param public: true(公开)；false（私有）
    function build_buckets_permission_url(params={id: 0, public: false}){
        let api = build_buckets_detail_api(params.id) + '?public=' + params.public;
        return build_url_with_domain_name(api);
    }

    //
    // 获取以存储桶开头的当前路径
    //传入参数无效或未传入参数时将尝试获取
    // 获取失败返回空字符串
    //
    function get_cur_path_startwith_bucket(params={bucket_name:'', dir_path: ''}) {
        let obj = params;
        //传入参数无效或未传入
        if(!params.hasOwnProperty('bucket_name') || (params.bucket_name==='')){
            obj = get_bucket_name_and_cur_path();
        }

        if(!obj.bucket_name)
            return '';

        let path = obj.bucket_name + '/';
        if (obj.dir_path)
            path = path + obj.dir_path + '/';
        return path;
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
        get_buckets_and_render();
    };

    //网页内容显示区div
    $content_display_div = $("#content-display-div");

    //
    // 创建存储桶按钮点击事件
    //
    $("#content-display-div").on('click', "#btn-new-bucket", on_create_bucket);//对话框方式



    //
    // 创建新的存储桶点击事件处理（对话框方式）
    //
    function on_create_bucket(){
        swal({
            title: '请输入一个符合DNS标准的存储桶名称，可输入英文字母(不区分大小写)、数字和-',
            input: 'text',
            inputAttributes: {
                autocapitalize: 'off'
            },
            showCloseButton: true,
            showCancelButton: true,
            confirmButtonText: '创建',
            showLoaderOnConfirm: true,
            preConfirm: (input_name) => {
                return $.ajax({
                    url: build_url_with_domain_name('/api/v1/buckets/'),
                    type: 'post',
                    data: {'name': input_name},
                    timeout: 200000,
                }).done((result) => {
                    if (result.code === 201){
                        return result;
                    }else{
                        swal.showValidationMessage(
                        `Request failed: ${result.code_text}`
                        );
                    }
                })
            },
            allowOutsideClick: () => !swal.isLoading()
        }).then(
            (result) => {
                if (result.value) {
                    show_warning_dialog(`创建存储桶“${result.value.data.name}”成功`, 'success').then(() => {
                        item_html = render_bucket_item(result.value.bucket);
                        $("#content-display-div #bucket-table tr:eq(0)").after(item_html);
                    } )
                }
             },
            (error) => {
                if(error.status<500)
                    show_warning_dialog(`创建失败:`+ error.responseJSON.code_text);
                else
                    show_warning_dialog(`创建失败:` + error.statusText);
            }
        )
    }


    //
    // 全选/全不选
    //
    $("#content-display-div").on('click', ':checkbox[data-check-target]', function () {
        let target = $(this).attr('data-check-target');
        let btn_del_bucket = $('#btn-del-bucket');
        if ($(this).prop('checked')) {
            $(target).prop('checked', true); // 全选
            $(target).parents('tr').addClass('danger'); // 选中时添加 背景色类
            if (is_exists_checked()){
                btn_del_bucket.removeClass('disabled');   //鼠标悬停时，使按钮表现为可点击状态
                btn_del_bucket.attr('disabled', false); //激活对应按钮
            }
        } else {
            $(target).prop('checked', false); // 全不选
            $(target).parents('tr').removeClass('danger');// 不选中时移除 背景色类
            btn_del_bucket.addClass('disabled'); //鼠标悬停时，使按钮表现为不可点击状态
            btn_del_bucket.attr('disabled', true);//失能对应按钮
        }
    });

    //
    // 表格中每一行单选checkbox
    //
    $("#content-display-div").on('click', '.item-checkbox', function () {
        let btn_del_bucket = $('#btn-del-bucket');
        if ($(this).prop('checked')){
            $(this).parents('tr').addClass('danger');
            btn_del_bucket.removeClass('disabled');
            btn_del_bucket.attr('disabled', false); //激活对应按钮
        }else{
            $(this).parents('tr').removeClass('danger');
            if (!is_exists_checked()){
                btn_del_bucket.addClass('disabled');
                btn_del_bucket.attr('disabled', true); //失能对应按钮
            }
        }
    })


    //
    // 检测是否有选中项
    //
    function is_exists_checked() {
        if ($(".item-checkbox:checked").size() === 0)
            return false;
        else
            return true;
    }

    //
    // 删除存储桶按钮
    //
    $("#content-display-div").on('click', '#btn-del-bucket', function () {
        if(!is_exists_checked()){
            show_warning_dialog('请先选择要删除的存储桶');
            return;
        }

        show_confirm_dialog({
            title: '确认删除选中的存储桶？',
            ok_todo: delete_selected_buckets,
        })
    });


    //
    // 删除选中的存储桶
    //
    function delete_selected_buckets(){
        //获取选中的存储桶的id
        var arr = new Array();
        let bucket_list_checked = $("#content-display-div #bucket-table #bucket-list-item :checkbox:checked");
        bucket_list_checked.each(function (i) {
            arr[i] = $(this).val();
        });
        if (arr.length > 0) {
            $.ajax({
                url: build_url_with_domain_name('/api/v1/buckets/0/'),
                type: 'delete',
                data: {
                    'ids': arr,// 存储桶id数组
                },
                traditional: true,//传递数组时需要设为true
                success: function (data) {
                    bucket_list_checked.parents('tr').remove();
                    show_auto_close_warning_dialog('已成功删除存储桶', 'success', 'top-end');
                },
                error: function (err) {
                    show_auto_close_warning_dialog('删除失败,' + err.status + ':' + err.statusText, 'error');
                },
            })
        }
    }


    //
    // 存储桶私有或公有访问权限设置按钮事件
    //
    $("#content-display-div").on('click', '#btn-public-bucket', function () {
        if(!is_exists_checked()){
            show_warning_dialog('请先选择存储桶');
            return;
        }

        (async function() {
            const {value: result} = await Swal({
                title: '选择权限',
                input: 'radio',
                inputOptions: {
                    'true': '公开',
                    'false': '私有',
                },
                showCancelButton: true,
                inputValidator: (value) => {
                    return !value && 'You need to choose something!'
                }
            });

            if (result) {
                selected_buckets_permission_set(result === 'true');
            }
        })();
    });

    //
    // 存储桶私有或公有访问权限设置
    //
    function selected_buckets_permission_set(publiced=false){
        //获取选中的存储桶的id
        var arr = new Array();
        let bucket_list_checked = $("#content-display-div #bucket-table #bucket-list-item :checkbox:checked");
        bucket_list_checked.each(function (i) {
            arr[i] = $(this).val();
        });
        if (arr.length > 0) {
            $.ajax({
                url: build_buckets_permission_url({id: 0, public: publiced}),
                type: 'patch',
                data: {
                    'ids': arr,// 存储桶id数组
                },
                traditional: true,//传递数组时需要设为true
                success: function (data) {
                    show_auto_close_warning_dialog('成功设置存储桶访问权限', 'success', 'center');
                },
                error: function (err) {
                    show_auto_close_warning_dialog('设置存储桶访问权限失败,' + err.status + ':' + err.statusText, 'error');
                },
            })
        }
    }

    //
    // 单个存储桶列表项渲染模板
    //
    let render_bucket_item = template.compile(`
        <tr class="active" id="bucket-list-item">
            <td><input type="checkbox" class="item-checkbox" value="{{ $data['id'] }}"></td>
            <td><span class="glyphicon glyphicon-oil"></span><span>  </span><a href="#" id="bucket-list-item-enter" bucket_name="{{ $data['name'] }}">{{ $data['name'] }}</a>
            <td>{{ $data['created_time'] }}</td>
            <td>{{ $data['access_permission'] }}</td>
        </tr>
    `);

    //
    //存储桶列表视图渲染模板
    //
    let render_bucket_view = template.compile(`
        <div class="container-fluid">
            <div class="row">
                <div class="col-xs-12 col-sm-12">
                    <div>
                        <button class="btn btn-primary" id="btn-new-bucket"><span class="glyphicon glyphicon-plus"></span> 创建存储桶
                        </button>
                        <button class="btn btn-danger disabled" id="btn-del-bucket" disabled="disabled" ><span class="glyphicon glyphicon-trash"></span> 删除存储桶</button>
                        <!--<button class="btn btn-warning disabled">清空存储桶</button>-->
                        <button class="btn btn-success" id="btn-public-bucket">公开</button>
                    </div>
                </div>
            </div>
            <hr style=" height:1px;border:1px;border-top:1px solid #185598;"/>
            <div class="row">
                <div class="col-xs-12 col-sm-12">
                    <table class="table table-hover" id="bucket-table">
                        <tr class="bg-info">
                            <th><input type="checkbox" data-check-target=".item-checkbox" /></th>
                            <th>存储桶名称</th>
                            <th>创建时间</th>
                            <th>访问权限</th>
                        </tr>
                        {{if buckets}}
                            {{ each buckets }}
                                <tr class="active" id="bucket-list-item">
                                    <td><input type="checkbox" class="item-checkbox" value="{{ $value.id }}"></td>
                                    <td><span class="glyphicon glyphicon-oil"></span><span>  </span><a href="#" id="bucket-list-item-enter" bucket_name="{{ $value.name }}">{{ $value.name }}</a>
                                    </td>
                                    <td>{{ $value.created_time }}</td>
                                    <td>{{ $value.access_permission }}</td>
                                </tr>
                            {{/each}}
                        {{/if}}
                    </table>
                     {{if buckets.length === 0}}
                          <p class="text-info text-center">肚子空空如也哦 =^_^=</p>
                     {{/if}}
                </div>
            </div>
            
            {{if (previous || next)}}
            <div class="row">
                <div class="col-xs-12 col-sm-12">
                   <nav aria-label="...">
                      <ul class="pager">
                        {{if previous}}
                            <li><a id="page_previous_buckets" href="{{previous}}"><span aria-hidden="true">&larr;</span>上页</a></li>
                        {{/if}}
                        {{if !previous}}
                            <li class="disabled"><a><span aria-hidden="true">&larr;</span>上页</a></li>
                        {{/if}}
                        
                        {{if page}}
                            <li>第{{page.current}}页 共{{page.final}}页</li>
                        {{/if}}
                        
                        {{if next}}
                            <li><a id="page_next_buckets" href="{{next}}">下页<span aria-hidden="true">&rarr;</span></a></li>
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

    //
    // 存储桶列表上一页Previous点击事件
    //
    $("#content-display-div").on("click", '.pager #page_previous_buckets', function (e) {
        e.preventDefault();
        url = $(this).attr('href');
        get_buckets_and_render(url);
    });

    //
    // 存储桶列表下一页Next点击事件
    //
    $("#content-display-div").on("click", '.pager #page_next_buckets', function (e) {
        e.preventDefault();
        url = $(this).attr('href');
        get_buckets_and_render(url);
    });


    //
    // 获取存储桶列表并渲染
    //
    function get_buckets_and_render(url=""){
        if(url !== "")
            get_content_and_render(url, render_bucket_view);
        else
            get_content_and_render(build_url_with_domain_name('api/v1/buckets/'), render_bucket_view);
    }

    //
    // 存储桶菜单点击事件处理
    //
    $("#nav-bucket-view").on("click", function (e) {
        e.preventDefault();
        get_buckets_and_render();
    });


    //
    // 面包屑路径导航home点击进入事件处理
    //
    $("#content-display-div").on("click", '#btn-path-bucket', function (e) {
        e.preventDefault();
        get_buckets_and_render();
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
                            <a href="#" id="btn-path-bucket">存储桶</a>
                            <span>></span>
                            <li><a href="" id="btn-path-item" bucket_name="{{ $data['bucket_name']}}"  dir_path="">{{ $data['bucket_name']}}</a></li>
                            {{each breadcrumb}}
                                <li><a href=""  id="btn-path-item" bucket_name="{{ $data['bucket_name']}}"  dir_path={{$value[1]}}>{{ $value[0] }}</a></li>
                            {{/each}}
                        </ol>
                    </div>
                    <hr>
                    <div>
                        <button class="btn btn-info" id="btn-new-directory"><span class="glyphicon glyphicon-plus"></span>创建文件夹</button>
                        <button class="btn btn-primary" id="btn-upload-file" bucket_name="{{ $data['bucket_name'] }}" cur_dir_path="{{ $data['dir_path'] }}">上传文件</button>
                        <button class="btn btn-success" id="btn-path-item" bucket_name="{{ $data['bucket_name'] }}" dir_path="{{ $data['dir_path'] }}"><span class="glyphicon glyphicon-refresh"></span></button>
                        <div class="progress text-warning" id="upload-progress-bar" style="display: none;">
                            <div class="progress-bar"  role="progressbar" aria-valuenow="0" aria-valuemin="0"
                                 aria-valuemax="100" style="min-width: 2em;width: 0%;">
                                0%
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            <hr style=" height:1px;border:1px;border-top:1px solid #185598;"/>
            <div class="row">
                <div class="col-xs-12 col-sm-12 table-responsive">
                    <table class="table" id="bucket-files-table">
                        <tr class="bg-info">
                            <th><input type="checkbox" data-check-target=".item-checkbox" /></th>
                            <th>名称</th>
                            <th>上传时间</th>
                            <th>大小</th>
                            <th>权限</th>
                            <th></th>
                        </tr>
                        {{each files}}
                            <tr class="bucket-files-table-item">
                                <td><input type="checkbox" class="item-checkbox" value=""></td>
                                <!--文件-->
                                {{ if $value.fod }}
                                    <td class="bucket-files-table-item">
                                        <span class="glyphicon glyphicon-file"></span>
                                        <a href="#" id="bucket-files-item-enter-file" download_url="{{$value.download_url}}">{{ $value.na }}</a>
                                    </td>
                                    <td>{{ $value.ult }}</td>
                                    <td>{{ $value.si }}</td>
                                    <td>{{ $value.access_permission}}</td>
                                {{/if}}
                                {{ if !$value.fod }}
                                    <td>
                                        <span class="glyphicon glyphicon-folder-open"></span>
                                        <a href="#" id="bucket-files-item-enter-dir" dir_path="{{$value.na}}"><strong class="bucket-files-table-item" >{{ $value.dir_name }}</strong></a>
                                    </td>
                                    <td>{{ $value.ult }}</td>
                                    <td>--</td>
                                    <td>--</td>
                                {{/if}}
                                <td>
                                    <li class="dropdown btn">
                                        <a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-haspopup="true"
                                   aria-expanded="false">操作<span class="caret"></span></a>
                                        <ul class="dropdown-menu">
                                            <!--目录-->
                                            {{ if !$value.fod }}
                                                <li class="btn-info"><a href="" id="bucket-files-item-enter-dir" dir_path="{{$value.na}}">打开</a></li>
                                                <li class="btn-danger"><a href="" id="bucket-files-item-delete-dir" dir_path="{{$value.na}}">删除</a></li>
                                            {{/if}}
                                            <!--文件-->
                                            {{ if $value.fod }}
                                                <li class="btn-success"><a id="bucket-files-item-download" href="{{$value.download_url}}" >下载</a></li>
                                                <li class="btn-danger"><a id="bucket-files-item-delete" href="" filename="{{$value.na}}">删除</a></li>
                                                <li class="btn-info"><a id="bucket-files-obj-share" href="" bucket_name="{{ $data['bucket_name']}}"  dir_path="{{$data['dir_path']}}" filename="{{$value.na}}">分享公开</a></li>
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


    //
    // 单个存储桶文件列表单个文件对象项渲染模板
    //
    let render_bucket_file_item = template.compile(`
        <tr class="bucket-files-table-item">
            <td><input type="checkbox" class="item-checkbox" value=""></td>
            <td class="bucket-files-table-item">
                <span class="glyphicon glyphicon-file"></span>
                <a href="#" id="bucket-files-item-enter-file"  download_url="{{obj.download_url}}">{{ obj.na }}</a>
            </td>
            <td>{{ obj.ult }}</td>
            <td>{{ obj.si }}</td>
            <td>{{ obj.access_permission }}</td>
            <td>
                <li class="dropdown btn">
                    <a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-haspopup="true"
               aria-expanded="false">操作<span class="caret"></span></a>
                    <ul class="dropdown-menu">
                        <li class="btn-success"><a id="bucket-files-item-download" href="{{obj.download_url}}" >下载</a></li>
                        <li class="btn-danger"><a id="bucket-files-item-delete" href="" filename="{{obj.na}}">删除</a></li>
                        <li class="btn-info"><a id="bucket-files-obj-share" href="" bucket_name="{{ $data['bucket_name']}}"  dir_path="{{$data['dir_path']}}" filename="{{obj.na}}">分享公开</a></li>
                    </ul>
                </li>
            </td>
        </tr>
    `);

    // 获取文件对象信息，并渲染文件对象列表项
    function get_file_info_and_list_item_render(url, render_bucket_file_item){
        $.ajax({
            type: 'get',
            url: url,
            // async: false,
            success: function(data,status,xhr){
                let html = render_bucket_file_item(data);
                $('#bucket-files-table tr:first').after(html);
            }
        });
    }

    //
    // 面包屑路径导航点击进入事件处理
    //
    $("#content-display-div").on("click", '#btn-path-item', function (e) {
        e.preventDefault();
        bucket_name = $(this).attr('bucket_name');
        dir_path = $(this).attr('dir_path');

        let url = build_dir_detail_url({
            bucket_name: bucket_name,
            dir_path: dir_path
        });
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
    // 存储桶文件删除事件处理
    //
    $("#content-display-div").on("click", '#bucket-files-item-delete', function (e) {
        e.preventDefault();

        let list_item_dom = $(this).parents("tr.bucket-files-table-item");
        filename = $(this).attr("filename");
        obj = get_bucket_name_and_cur_path();
        obj.filename = filename;

        let url = build_obj_detail_url(obj);

        show_confirm_dialog({
            title: '确定要删除吗？',
            ok_todo: function() {
                delete_one_file(url, function () {
                    list_item_dom.remove();
                })
            },
        });
    });

    //
    // 文件对象分享公开点击事件处理
    //
    $("#content-display-div").on("click", '#bucket-files-obj-share', function (e) {
        e.preventDefault();

        let obj = {
            bucket_name: $(this).attr("bucket_name"),
            dir_path: $(this).attr("dir_path"),
            filename: $(this).attr("filename")
        };

        let detail_url = build_obj_detail_url(obj);

        (async function() {
            const {value: key} = await Swal({
                title: '请选择公开时间',
                input: 'select',
                inputOptions: {
                    '0': '永久公开',
                    '1': '1天',
                    '7': '7天',
                    '30': '30天',
                    '-1': '私有'
                },
                showCancelButton: true,
                inputValidator: (value) => {
                    return !value && '请选择一个选项';
                }
            });

            if (key) {
                let share_url = build_obj_share_url({
                    detail_url: detail_url,
                    share: true,
                    days: key
                });
                set_obj_shared(share_url);
            }
        })();
    });

    //
    // 文件对象分享公开权限设置
    //
    function set_obj_shared(url) {
        $.ajax({
            type: 'patch',
            url: url,
            success: function (data,status,xhr) {
                show_auto_close_warning_dialog('分享公开设置成功', type='success');
            },
            error: function (error) {
                show_warning_dialog('分享公开设置失败！', type='error');
            }
        })
    }


    //
    // 删除一个文件
    //
    function delete_one_file(url, success_do){
        swal.showLoading();
        $.ajax({
            type: 'delete',
            url: url,
            success: function(data,status,xhr){
                swal.close();
                success_do();
                show_auto_close_warning_dialog('删除成功', type='success');
            },
            error: function (error) {
                swal.close();
                show_warning_dialog('删除失败！', type='error');
            }
        });
    }


    //
    // 删除文件夹事件处理
    //
    $("#content-display-div").on("click", '#bucket-files-item-delete-dir', function (e) {
        e.preventDefault();
        let list_item_dom = $(this).parents("tr.bucket-files-table-item");
        bucket_name = get_bucket_name_and_cur_path().bucket_name;
        dir_path = $(this).attr('dir_path');

        let url = build_dir_detail_url({
            bucket_name: bucket_name,
            dir_path: dir_path
        });

        show_confirm_dialog({
            title: '确定要删除吗？',
            ok_todo: function() {
                delete_one_directory(url, function () {
                    list_item_dom.remove();
                });
            },
        });
    });

    //
    // 删除一个文件夹
    //
    function delete_one_directory(url, success_do){
        swal.showLoading();
        $.ajax({
            type: 'delete',
            url: url,
            success: function(data,status,xhr){
                swal.close();
                success_do();
                show_auto_close_warning_dialog('删除成功', type='success');
            },
            error: function (error,status) {
                swal.close();
                if ((err.status < 500) && err.responseJSON.hasOwnProperty('code_text'))
                    show_warning_dialog('删除失败:'+ error.responseJSON.code_text, type='error');
                else
                    show_warning_dialog('上传文件发生错误,'+ err.statusText);
            }
        });
    }


    //
    // 存储桶列表项点击进入事件处理
    //
    $("#content-display-div").on("click", '#bucket-list-item-enter', function (e) {
        e.preventDefault();
        bucket_name = $(this).attr('bucket_name');

        let url = build_dir_detail_url({
            bucket_name: bucket_name,
            dir_path: ''
        });
        get_bucket_files_and_render(url);
    });


    //
    // 存储桶列表文件夹点击进入事件处理
    //
    $("#content-display-div").on("click", '#bucket-files-item-enter-dir', function (e) {
        e.preventDefault();
        let bucket_name = get_bucket_name_and_cur_path().bucket_name;
        let dir_path = $(this).attr('dir_path');

        let url = build_dir_detail_url({
            bucket_name: bucket_name,
            dir_path: dir_path
        });
        get_bucket_files_and_render(url);
    });


    //
    // 存储桶列表文件对象点击进入事件处理
    //
    $("#content-display-div").on("click", '#bucket-files-item-enter-file', function (e) {
        e.preventDefault();
        let obj = get_bucket_name_and_cur_path();
        obj.filename = $(this).text();
        let url = build_obj_info_api(obj);
        url = build_url_with_domain_name(url);
        get_file_obj_info_and_render(url);
    });


    //
    // 获取存储桶文件列表并渲染
    //
    function get_bucket_files_and_render(url){
        get_content_and_render(url, render_bucket_files_view);
    }


    //
    // 获取文件对象信息并渲染
    //
    function get_file_obj_info_and_render(url){
        get_content_and_render(url, render_file_obj_info_view);
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

    //
    //存储桶文件对象详细信息视图渲染模板
    //
    let render_file_obj_info_view = template.compile(`
        <div class="container-fluid">
        <div class="row">
            <div class="col-xs-12 col-sm-12">
                <div>
                    <ol class="breadcrumb">
                        <a href="#" id="btn-path-bucket">存储桶</a>
                        <span>></span>
                        <li><a href="" id="btn-path-item" bucket_name="{{ $data['bucket_name']}}"  dir_path="">{{ $data['bucket_name']}}</a></li>
                        {{each breadcrumb}}
                            <li><a href=""  id="btn-path-item" bucket_name="{{ $data['bucket_name']}}"  dir_path="{{$value[1]}}">{{ $value[0] }}</a></li>
                        {{/each}}
                    </ol>
                    <p><h3>{{ obj.na }}</h3></p>
                </div>
            </div>
            <div class="col-sm-12"><hr style=" height:1px;border:1px;border-top:1px solid #185598;" /></div>
            <div class="col-xs-12 col-sm-12">
                <div>
                    <a class="btn btn-info" href="{{ obj.download_url }}">下载</a>
                    <button class="btn btn-warning" id="bucket-files-obj-share" bucket_name="{{ $data['bucket_name']}}"  dir_path="{{$data['dir_path']}}" filename="{{obj.na}}">分享公开</button>
                </div>
                <hr/>
                <div>
                    <strong>对象名称：</strong>
                    <p>{{ obj.na }}</p>
                    <p><strong>对象大小：</strong>{{ obj.si }}</p>                   
                    <p><strong>创建日期：</strong>{{ obj.ult }}</p>                   
                    <p><strong>修改日期：</strong>{{ obj.upt }}</p> 
                                      
                    {{if obj.dlc}}
                        <p><strong>下载次数：</strong>{{ obj.dlc }}</p>
                    {{else if !obj.dlc}}
                        <p><strong>下载次数：</strong>0</p>
                    {{/if}}
                    
                    <p><strong>访问权限：</strong>{{obj.access_permission}}</p>
                    {{if obj.sh}}
                        {{if obj.stl}}
                            <p><strong>公共访问终止时间：</strong>{{obj.set}}</p>
                        {{else if !obj.stl}}
                            <p><strong>公终止时间：</strong>永久公开</p>
                        {{/if}}
                    {{/if}}
                    <strong>下载连接：</strong>
                    <p><a href="{{ obj.download_url }}">{{ obj.download_url }}</a></p>
                </div>
            </div>
        </div>
    </div>
    `);

    //
    // 上传文件按钮
    //
    $("#content-display-div").on("click", '#btn-upload-file',
        async function () {
            // $("#div-upload-file").show();

            const {value: file} = await swal({
                title: '选择文件',
                input: 'file',
                showCancelButton: true,
                inputAttributes: {
                    'accept': '*',
                    'aria-label': 'Upload your select file'
                }
            });
            if (file) {
                const reader = new FileReader;
                reader.onload = (e) => {
                    uploadOneFile(file);//上传文件
                };
                reader.readAsDataURL(file);
            }
            else if(file === null){
                show_warning_dialog("没有选择文件，请先选择一个文件");
            }
        }
    );

    //
    // 上传一个文件
    //
    function uploadOneFile(file) {
        obj = get_bucket_name_and_cur_path();
        if(!obj.bucket_name){
            show_warning_dialog('上传文件失败，无法获取当前存储桶下路径');
            return;
        }

        let url = build_obj_detail_url({
            bucket_name: obj.bucket_name,
            dir_path: obj.dir_path,
            filename: file.name
        });
        beforeFileUploading();
        uploadFile(url, file, 0);
    }


    //
    //文件上传
    //
    function uploadFile(put_url, file, offset = 0) {
        // 断点续传记录检查

        // 分片上传文件
        uploadFileChunk(put_url, file, offset);
    }

    //
    //分片上传文件
    //
    function uploadFileChunk(url, file, offset) {
        let chunk_size = 2 * 1024 * 1024;//5MB
        let end = get_file_chunk_end(offset, file.size, chunk_size);
        //进度条
        fileUploadProgressBar(offset, file.size);

        //文件上传完成
        if (end === -1){
            //进度条
            fileUploadProgressBar(0, 1, true);
            endFileUploading();
            show_auto_close_warning_dialog('文件已成功上传', 'success', 'top-end');
            // 如果上传的文件在当前页面的列表中，插入文件列表
            success_upload_file_append_list_item(url, file.name);

            return;
        }
        var chunk = file.slice(offset, end);
        var formData = new FormData();
        formData.append("chunk_offset", offset);
        formData.append("chunk", chunk);
        formData.append("chunk_size", chunk.size);
        formData.append("overwrite", false);

        $.ajax({
            url: url,
            type: "PUT",
            data: formData,
            contentType: false,//必须false才会自动加上正确的Content-Type
            processData: false,//必须false才会避开jQuery对 formdata 的默认处理,XMLHttpRequest会对 formdata 进行正确的处理
            success: function (data, textStatus, request) {
                // request.getResponseHeader('Server');
                offset = end;
                uploadFileChunk(url, file, offset);
            },
            error: function (err) {
                if ((err.status < 500) && err.responseJSON.hasOwnProperty('code_text'))
                    show_warning_dialog('上传文件发生错误,'+ err.responseJSON.code_text);
                else
                    show_warning_dialog('上传文件发生错误,'+ err.statusText);

                endFileUploading();
            },
        })
    }

    //
    // 成功上传文件后，插入文件列表项
    function success_upload_file_append_list_item(obj_url, filename) {
        let obj = get_bucket_name_and_cur_path();
        if(!obj.bucket_name){
            return;
        }

        params = {
            bucket_name: obj.bucket_name,
            dir_path: obj.dir_path,
            filename: filename
        };
        let cur_url = build_obj_detail_url(params);

        if (cur_url === obj_url){
            let info_url = build_obj_info_api(params);
            get_file_info_and_list_item_render(info_url, render_bucket_file_item);
        }
    }

    //
    // 文件块结束字节偏移量
    //-1: 文件上传完成
    function get_file_chunk_end(offset, file_size, chunk_size) {
        let end = null;
        if (offset < file_size) {
            if ((offset + chunk_size) > file_size) {
                end = file_size;
            } else {
                end = offset + chunk_size;
            }
        } else if (offset >= file_size) {
            end = -1;
        }
        return end
    }


    //
    // 从当前路径url中获取存储桶名和目录路径
    //
    function get_bucket_name_and_cur_path(){
        let $btn = $('#btn-upload-file');
        if(!$btn)
            return {
                'bucket_name': '',
                'dir_path': ''
            };
        let bucket_name = $btn.attr('bucket_name');
        let dir_path = $btn.attr('cur_dir_path');
        return {
            'bucket_name': bucket_name,
            'dir_path': dir_path
        }
    }


    //
    // 进度条设置
    //
    function setProgressBar(obj_bar, width, hide=false){
        width = Math.floor(width);
        var $bar = $(obj_bar);
        percent = width + '%';
        $bar.children().attr({"style": "min-width: 2em;width: " + percent + ";"});
        $bar.children().text(percent);
        if (hide === true)
            $bar.hide();
        else
            $bar.show();
    }


    //
    // 文件上传进度条
    //
    function fileUploadProgressBar(now, total, hide=false) {
        var percent = 100 * now / total;
        if (percent > 100) {
            percent = 100;
        }
        setProgressBar($("#upload-progress-bar"), percent, hide);
    }


    //
    // 开始上传文件前设置
    //
    function beforeFileUploading(){
        // 进度条
        fileUploadProgressBar(0, 100, false);
        // 失能上传文件按钮
        let $btn = get_btn_file_upload();
        $btn.addClass('disabled');
        $btn.attr('disabled',true);
    }

    //
    // 上传文件完成或失败后设置
    //
    function endFileUploading(){
         // 进度条
        fileUploadProgressBar(0, 100, true);
        // 失能上传文件按钮
        let $btn = get_btn_file_upload();
        $btn.removeClass('disabled');
        $btn.attr('disabled',false);
    }

    //
    // 获取文件上传按钮节点元素
    //
    function get_btn_file_upload() {
        return $("#btn-upload-file");
    }


    //
    // 创建文件夹点击事件处理（对话框方式）
    //
    $("#content-display-div").on('click', '#btn-new-directory', on_create_directory);
    function on_create_directory(){
        swal({
            title: '请输入一个文件夹名称',
            input: 'text',
            inputAttributes: {
                autocapitalize: 'off'
            },
            showCancelButton: true,
            confirmButtonText: '创建',
            showLoaderOnConfirm: true,
            preConfirm: (input_name) => {
                let obj = get_bucket_name_and_cur_path();
                let bucket_name = obj.bucket_name;
                let dir_path = obj.dir_path;
                var formdata = new FormData();
                formdata.append('bucket_name', bucket_name);
                formdata.append('dir_path', dir_path);
                formdata.append('dir_name', input_name);
                return $.ajax({
                    url: build_url_with_domain_name(get_dir_base_api()),
                    type: 'post',
                    data: formdata,
                    timeout: 200000,
                    contentType: false,//必须false才会自动加上正确的Content-Type
                    processData: false,//必须false才会避开jQuery对 formdata 的默认处理,XMLHttpRequest会对 formdata 进行正确的处理
                    success: (result) => {
                        if (result.code === 201){
                            return result;
                        }else{
                            swal.showValidationMessage(
                            `Request failed: ${result.code_text.error_text}`
                            );
                        }
                    },
                    error: (error) => {
                        swal.showValidationMessage(
                            `Request failed: ${error.responseJSON.error_text}`
                        );
                    },
                    headers: {'X-Requested-With': 'XMLHttpRequest'},//django判断是否是异步请求时需要此响应头
                    clearForm: false,//禁止清除表单
                    resetForm: false //禁止重置表单
                });
            },
            allowOutsideClick: () => !swal.isLoading()
        }).then(
            (result) => {
                if (result.value) {
                    show_warning_dialog(`创建文件夹“${result.value.data.dir_name}”成功`, 'success').then(() => {
                        // location.reload(true);// 刷新当前页面
                        let html = render_bucket_dir_item(result.value);
                        $("#bucket-files-table tr:eq(0)").after(html);
                    } )
                }
             },
            (error) => {
                if(error.status<500)
                    show_warning_dialog(`创建失败:`+ error.responseJSON.code_text.error_text);
                else
                    show_warning_dialog(`创建失败:` + error.statusText);
            }
        )
    }

    //
    // 单个存储桶目录列表项渲染模板
    //
    let render_bucket_dir_item = template.compile(`
        <tr class="bucket-files-table-item">
            <td><input type="checkbox" class="item-checkbox" value=""></td>
            <td>
                <span class="glyphicon glyphicon-folder-open"></span>
                <a href="#" id="bucket-files-item-enter-dir" dir_path="{{dir.na}}"><strong class="bucket-files-table-item">{{ dir.dir_name }}</strong></a>
            </td>
            <td>{{ dir.ult }}</td>
            <td>--</td>
            <td>--</td>
            <td>
                <li class="dropdown btn">
                    <a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-haspopup="true"
               aria-expanded="false">操作<span class="caret"></span></a>
                    <ul class="dropdown-menu">
                         <li class="btn-info"><a href="" id="bucket-files-item-enter-dir" dir_path="{{dir.na}}">打开</a></li>
                         <li class="btn-danger"><a href="" id="bucket-files-item-delete-dir" dir_path="{{dir.na}}">删除</a></li>
                    </ul>
                </li>
            </td>
        </tr>
    `);


})();



