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

        let path = encode_paths([params.bucket_name, params.dir_path, params.filename]);
        return get_obj_base_api() + path + '/';
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
        let path = encode_paths([params.bucket_name, params.dir_path]);
        return get_dir_base_api() + path + '/';
    }

    //构建目录detail url
    function build_dir_detail_url(params={bucket_name: '', dir_path: '', dir_name: ''}){
        if (params.hasOwnProperty('dir_name')){
            let dir_path = params.dir_path;
            let dir_name = params.dir_name;
            delete params.dir_name;
            if (dir_path)
                params.dir_path = dir_path + '/' + dir_name;
            else
                params.dir_path = dir_name;
        }
        let api = build_dir_detail_api(params);
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
    //@param public: 1(公有)，2(私有)，3（公有可读可写）
    function build_buckets_permission_url(params={id: 0, public: 2, ids:[]}){
        let bid = params.id;
        delete params.id;
        let api = build_buckets_detail_api(bid) + '?' + $.param(params, true);
        return build_url_with_domain_name(api);
    }

    //
    // 对象移动重命名base api
    //
    function get_move_base_api() {
        return "/api/v1/move/";
    }

    //
    // 构建移动和重命名url
    //
    function build_move_rename_url(paths=[], params={ move_to: '', rename: ''}) {
        let obj_path = encode_paths(paths);
        let param_str = encode_params(params);
        let api = get_move_base_api() + obj_path + '/?' + param_str;
        return build_url_with_domain_name(api);
    }

    //
    // 对象metadata base api
    //
    function get_metadata_base_api() {
        return "/api/v1/metadata/";
    }

    //
    //构建对象元数据api
    //错误返回空字符串
    function build_metadata_api(params={bucket_name: '', dir_path: '', filename: ''}){
        if (!params.hasOwnProperty('bucket_name') || !params.hasOwnProperty('dir_path') || !params.hasOwnProperty('filename'))
            return '';

        let path = encode_paths([params.bucket_name, params.dir_path, params.filename]);
        return get_metadata_base_api() + path + '/';
    }

    //
    // bucket ftp base api
    //
    function get_ftp_base_api() {
        return "/api/v1/ftp/";
    }

    //
    //构建bucket ftp api
    function build_ftp_patch_url(params={bucket_name:'', enable: '', password: ''}) {

        let name = params.bucket_name;
        delete params.bucket_name;
        let param_str = encode_params(params);
        let api = get_ftp_base_api() + name +  '/?' + param_str;
        return build_url_with_domain_name(api);
    }

    //构建查询分享链接url
    function build_share_detail_url(params={bucket_name:'', path: ''}) {
        let api = 'api/v1/share/' + encode_paths([params.bucket_name, params.path]);
        return build_url_with_domain_name(api);
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
        get_buckets_and_render();
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
        path = path.strip('/');
        if (path !== '') {
            let arr = path.split('/');
            for (let i = 0, j = arr.length; i < j; i++) {
                breadcrumb.push([arr[i], arr.slice(0, i + 1).join('/')]);
            }
        }
        return breadcrumb;
    }

    function get_err_msg_or_default(xhr, default_msg) {
        let msg = default_msg;
        try {
            let data = xhr.responseJSON;
            if (data.hasOwnProperty('code_text')) {
                msg = default_msg + data.code_text;
            }
        } catch (e) {

        }
        return msg;
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

    //
    // 创建存储桶按钮点击事件
    //
    $("#content-display-div").on('click', "#btn-new-bucket", on_create_bucket);//对话框方式

    //
    // 创建新的存储桶点击事件处理（对话框方式）
    //
    function on_create_bucket(){
        Swal.fire({
            title: getTransText('请输入一个符合DNS标准的存储桶名称，可输入英文字母(不区分大小写)、数字和-'),
            input: 'text',
            inputValidator: (value) => {
                return (value.length<3 || value.length>63) && getTransText('请输入3-63个字符');
            },
            showCloseButton: true,
            showCancelButton: true,
            cancelButtonText: getTransText('取消'),
            confirmButtonText: getTransText('创建'),
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
                        Swal.showValidationMessage(
                        `Request failed: ${result.code_text}`
                        );
                    }
                })
            },
            allowOutsideClick: () => !Swal.isLoading()
        }).then(
            (result) => {
                if (result.value) {
                    let item_html = render_bucket_item(result.value.bucket);
                    $("#content-display-div #bucket-table tr:eq(0)").after(item_html);
                    show_warning_dialog(getTransText(`创建存储桶成功`), 'success');
                }
             },
            (error) => {
                let msg = getTransText(`创建失败`) + ";";
                if(error.status<500)
                    show_warning_dialog(msg + error.responseJSON.code_text);
                else
                    show_warning_dialog(msg + error.statusText);
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
    });


    //
    // 检测是否有选中项
    //
    function is_exists_checked() {
        return $(".item-checkbox:checked").length !== 0;
    }

    //
    // 删除存储桶按钮
    //
    $("#content-display-div").on('click', '#btn-del-bucket', function () {
        if(!is_exists_checked()){
            show_warning_dialog(getTransText('请先选择要删除的存储桶'));
            return;
        }

        show_confirm_dialog({
            title: getTransText('确认删除选中的存储桶吗'),
            text: getTransText('此操作是不可逆的！'),
            ok_todo: delete_selected_buckets,
        })
    });


    //
    // 删除选中的存储桶
    //
    function delete_selected_buckets(){
        //获取选中的存储桶的id
        let arr = [];
        let bucket_list_checked = $("#content-display-div #bucket-table #bucket-list-item :checkbox:checked");
        bucket_list_checked.each(function (i) {
            arr.push($(this).val());
        });
        if (arr.length > 0) {
            let b_id = arr[0];
            arr.splice(0,1);
            let url = '/api/v1/buckets/' + b_id + '/';
            if (arr.length > 0) {
                url = url + '?' + $.param({"ids": arr}, true);
            }
            $.ajax({
                url: build_url_with_domain_name(url),
                type: 'delete',
                success: function (data) {
                    bucket_list_checked.parents('tr').remove();
                    show_auto_close_warning_dialog(getTransText('已成功删除存储桶'), 'success', 'top-end');
                },
                error: function (err) {
                    show_auto_close_warning_dialog(getTransText('删除失败') + err.status + ':' + err.statusText, 'error');
                },
            })
        }
    }

    //
    // 单个存储桶列表项渲染模板
    //
    let render_bucket_item = template.compile(`
        <tr class="" id="bucket-list-item">
            <td><input type="checkbox" class="item-checkbox" value="{{ $data['id'] }}"></td>
            <td><i class="fa fa-database"></i><span>  </span><a href="#" id="bucket-list-item-enter" bucket_name="{{ $data['name'] }}">{{ $data['name'] }}</a>
            <td>{{ $imports.isoTimeToLocal($data['created_time']) }}</td>
            <td class="access-perms-enable">
                <span>{{ $data['access_permission'] }}</span>
                <span class="btn-share-bucket"><i class="fa fa-edit"></i></span>
            </td>
            <td class="ftp-enable">
                {{if $data['ftp_enable']}}
                    <span>{{$imports.getTransText('开启')}}</span>
                {{/if}}
                {{if !$data['ftp_enable']}}
                    <span>{{$imports.getTransText('关闭')}}</span>
                {{/if}}
                <span class="ftp-enable-btn" data-bucket-name="{{ $data['name'] }}"><i class="fa fa-edit"></i></span>
            </td>
            <td class="mouse-hover" data-bucket-name="{{ $data['name'] }}">
                <span class="mouse-hover-no-show">******</span>
                <span class="ftp-password-value mouse-hover-show">{{ $data['ftp_password'] }}</span>
                <i class="fa fa-edit mouse-hover-show ftp-password-edit"></i>
            </td>
            <td class="mouse-hover" data-bucket-name="{{ $data['name'] }}">
                <span class="mouse-hover-no-show">******</span>
                <span class="mouse-hover-show">{{ $data['ftp_ro_password'] }}</span>
                <i class="fa fa-edit mouse-hover-show ftp-ro-password-edit"></i>
            </td>
            <td class="bucket-remarks-edit" title="{{$imports.getTransText('双击修改备注')}}" data-bucket-id="{{ $data['id'] }}" style="max-width: 150px; word-wrap: break-word;">
                <span class="bucket-remarks-value">{{ $data['remarks'] }}</span>
            </td>
            <td>
                <bucket class="btn btn-sm btn-success btn-bucket-stats" data-bucket-name="{{ $data['name'] }}">{{$imports.getTransText('资源统计')}}</bucket>
            </td>
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
                        <button class="btn btn-primary" id="btn-new-bucket"><i class="fa fa-plus"></i>{{$imports.getTransText('创建存储桶')}}
                        </button>
                        <button class="btn btn-danger disabled" id="btn-del-bucket" disabled="disabled" ><i class="fa fa-trash"></i>{{$imports.getTransText('删除存储桶')}}</button>
                    </div>
                </div>
            </div>
            <hr style=" height:1px;border:1px;border-top:1px solid #185598;"/>
            <div class="row">
                <div class="col-xs-12 col-sm-12">
                    <table class="table table-hover" id="bucket-table">
                        <tr class="bg-light">
                            <th><input type="checkbox" data-check-target=".item-checkbox" /></th>
                            <th>{{$imports.getTransText('存储桶名称')}}</th>
                            <th>{{$imports.getTransText('创建时间')}}</th>
                            <th>{{$imports.getTransText('访问权限')}}</th>
                            <th>{{$imports.getTransText('FTP状态')}}</th>
                            <th>{{$imports.getTransText('FTP读写密码')}}</th>
                            <th>{{$imports.getTransText('FTP只读密码')}}</th>
                            <th style="max-width: 150px; word-wrap: break-word;">{{$imports.getTransText('备注')}}</th>
                            <th>{{$imports.getTransText('操作')}}</th>
                        </tr>
                        {{if buckets}}
                            {{set str_open = $imports.getTransText('开启')}}
                            {{set str_close = $imports.getTransText('关闭')}}
                            {{set str_db_passwd = $imports.getTransText('双击修改密码')}}
                            {{set str_db_remark = $imports.getTransText('双击修改备注')}}
                            {{set str_stat = $imports.getTransText('资源统计')}}
                            {{ each buckets }}
                                <tr class="" id="bucket-list-item">
                                    <td><input type="checkbox" class="item-checkbox" value="{{ $value.id }}"></td>
                                    <td><i class="fa fa-database"></i><span>  </span><a href="#" id="bucket-list-item-enter" bucket_name="{{ $value.name }}">{{ $value.name }}</a>
                                    </td>
                                    <td>{{ $imports.isoTimeToLocal($value.created_time) }}</td>
                                    <td>
                                        <span>{{ $value.access_permission }}</span>
                                        <span class="btn-share-bucket"><i class="fa fa-edit"></i></span>
                                    </td>
                                    <td class="ftp-enable">
                                        {{if $value.ftp_enable}}
                                            <span>{{str_open}}</span>
                                        {{/if}}
                                        {{if !$value.ftp_enable}}
                                            <span>{{str_close}}</span>
                                        {{/if}}
                                        <span class=" ftp-enable-btn" data-bucket-name="{{ $value.name }}"><i class="fa fa-edit"></i></span>
                                     </td>
                                    <td class="mouse-hover" data-bucket-name="{{ $value.name }}">
                                        <span class="mouse-hover-no-show">******</span>
                                        <span class="ftp-password-value mouse-hover-show">{{ $value.ftp_password }}</span>
                                        <i class="fa fa-edit mouse-hover-show ftp-password-edit"></i>
                                    </td>
                                    <td class="mouse-hover" data-bucket-name="{{ $value.name }}">
                                        <span class="mouse-hover-no-show">******</span>
                                        <span class="mouse-hover-show">{{ $value.ftp_ro_password }}</span>
                                        <i class="fa fa-edit mouse-hover-show ftp-ro-password-edit"></i>
                                    </td>
                                    <td class="bucket-remarks-edit" style="max-width: 150px; word-wrap: break-word;" title="{{str_db_remark}}" data-bucket-id="{{ $value.id }}">
                                        <span class="bucket-remarks-value">{{ $value.remarks }}</span>
                                    </td>
                                    <td>
                                        <bucket class="btn btn-sm btn-success btn-bucket-stats" data-bucket-name="{{ $value.name }}">{{str_stat}}</bucket>
                                    </td>
                                </tr>
                            {{/each}}
                        {{/if}}
                    </table>
                     {{if buckets.length === 0}}
                          <p class="text-info text-center">{{$imports.getTransText('肚子空空如也哦')}} =^_^=</p>
                     {{/if}}
                </div>
            </div>           
            {{if (previous || next)}}
            <div class="row">
                <div class="col-xs-12 col-sm-12">
                   <nav aria-label="Page navigation">
                      <ul class="pagination" style="margin:0;">
                        {{if previous}}
                            <li class="page-item"><a class="page-link" id="page_previous_buckets" href="{{previous}}"><span aria-hidden="true">&laquo;</span>{{$imports.getTransText('上页')}}</a></li>
                        {{/if}}
                        {{if !previous}}
                            <li class="page-item disabled"><a class="page-link"><span aria-hidden="true">&laquo;</span>{{$imports.getTransText('上页')}}</a></li>
                        {{/if}}                       
                        {{if page}}
                            <li class="disabled page-item"><spam class="page-link"><%= $imports.interpolate($imports.getTransText('第%s页 / 共%s页'), [page.current, page.final]) %></spam></li>
                        {{/if}}                       
                        {{if next}}
                            <li class="page-item"><a class="page-link" id="page_next_buckets" href="{{next}}">{{$imports.getTransText('下页')}}<span aria-hidden="true">&raquo;</span></a></li>
                        {{/if}}
                        {{if !next}}
                            <li class="page-item disabled"><a class="page-link">{{$imports.getTransText('下页')}}<span aria-hidden="true">&raquo;</span></a></li>
                        {{/if}}
                      </ul>
                    </nav>
                </div>
            </div>
            {{/if}}
        </div>
    `);

    //
    // 单个存储桶资源统计信息渲染模板
    let render_bucket_stats = template.compile(`
        <div>
            <table class="table table-bordered">
                <tr>
                    <td>{{$imports.getTransText("存储桶名称")}}：</td>
                    <td>{{ $data["bucket_name"] }}</td>
                </tr>
                <tr>
                    <td>{{$imports.getTransText("容量大小")}}：</td>
                    <td>{{ stats.space }}B ({{ $imports.sizeFormat(stats.space, "B") }})</td>
                </tr>
                <tr>
                    <td>{{$imports.getTransText('对象数量')}}：</td>
                    <td>{{ stats.count }}</td>
                </tr>
                <tr>
                    <td>{{$imports.getTransText("统计时间")}}：</td>
                    <td>{{ $imports.isoTimeToLocal($data["stats_time"]) }}</td>
                </tr>
            </table>
        </div>         
    `);

    // 存储桶备注信息修改
    $("#content-display-div").on("dblclick", '.bucket-remarks-edit', function (e) {
        e.preventDefault();
        let bucket_id = $(this).attr('data-bucket-id');
        let remarks = $(this).children('.bucket-remarks-value');
        let old_html = remarks.text();
        old_html = old_html.replace(/(^\s*) | (\s*$)/g,'');

        //如果已经双击过，正在编辑中
        if(remarks.attr('data-in-edit') === 'true'){
            return;
        }
        // 标记正在编辑中
        remarks.attr('data-in-edit', 'true');
        //创建新的input元素，初始内容为原备注信息
        let newobj = document.createElement('input');
        newobj.type = 'text';
        newobj.value = old_html;
        //设置该标签的子节点为空
        remarks.empty();
        remarks.append(newobj);
        newobj.setSelectionRange(0, old_html.length);
        //设置获得光标
        newobj.focus();
        //为新增元素添加光标离开事件
        newobj.onblur = function () {
            remarks.attr('data-in-edit', '');
            remarks.empty();
            let input_text = this.value;
            // 如果输入内容修改了
            if (input_text && (input_text !== old_html)){
                if (input_text.length > 255){
                    show_warning_dialog(getTransText('备注长度不得大于255个字符'), 'warning');
                    remarks.append(old_html);
                    return;
                }
                // 请求修改备注信息
                let url = build_url_with_domain_name('api/v1/buckets/' + bucket_id + '/remark/?remarks=' + input_text);
                $.ajax({
                    url: url,
                    type: "PATCH",
                    content_type: "application/json",
                    timeout: 10000,
                    success: function (res) {
                        if (res.code === 200) {
                            show_warning_dialog(getTransText('修改备注成功'), 'success');
                            remarks.append(input_text);
                        }
                    },
                    error: function (xhr, status) {
                        if (status === 'timeout') {
                            alert("timeout");
                        }else{
                            let msg = get_err_msg_or_default(xhr, 'request failed');
                            show_warning_dialog(getTransText('修改备注失败') + msg, 'error');
                        }
                        remarks.append(old_html);
                    }
                });
            }else{
                remarks.append(old_html);
            }
        };
    });

    //
    // 存储桶资源统计点击事件
    $("#content-display-div").on("click", '.btn-bucket-stats', function (e) {
        e.preventDefault();
        let bucket = $(this).attr("data-bucket-name");
        Swal.showLoading();
        $.ajax({
            url: build_url_with_domain_name("api/v1/stats/bucket/" + bucket + "/"),
            type: "get",
            timeout: 30000,
            success: function(data,status,xhr){
                Swal.close();
                let html = render_bucket_stats(data);
                Swal.fire({
                    title: getTransText('存储桶资源统计'),
                    html: html,
                    footer: getTransText('提示：数据非实时统计，有一定时间间隔')
                })
            },
            error: function (xhr, errtype, error) {
                Swal.close();
                if (errtype === 'timeout'){
                    show_warning_dialog('timeout', 'error');
                }else{
                    let msg = get_err_msg_or_default(xhr, "request failed，" + xhr.statusText);
                    show_warning_dialog(msg, 'error');
                }
            }
        });
    });

    //
    // 存储桶列表上一页Previous点击事件
    //
    $("#content-display-div").on("click", '#page_previous_buckets', function (e) {
        e.preventDefault();
        let url = $(this).attr('href');
        get_buckets_and_render(url);
    });

    //
    // 存储桶列表下一页Next点击事件
    //
    $("#content-display-div").on("click", '#page_next_buckets', function (e) {
        e.preventDefault();
        let url = $(this).attr('href');
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

    // 修改FTP读写和只读密码
    // @param dom_pw: 密码dom节点
    // @param pw_query_name: API中密码对应的query参数名称
    function changeFtpPassword(dom_pw, pw_query_name){
        let old_password = dom_pw.text();

        Swal.fire({
            title: getTransText('修改FTP密码'),
            input: 'text',
            inputValue: old_password,
            inputValidator: (value) => {
                return (value.length<6 || value.length>20) && getTransText('密码长度6-20个字符');
            },
            showCloseButton: true,
            showCancelButton: true,
            cancelButtonText: getTransText('取消'),
            confirmButtonText: getTransText('确定'),
            showLoaderOnConfirm: true,
            preConfirm: (value) => {
                if (value && (value !== old_password)){
                    let data = {};
                    data.bucket_name = dom_pw.parent().attr('data-bucket-name');
                    data[pw_query_name] = value;
                    let url = build_ftp_patch_url(data);
                    return $.ajax({
                        url: url,
                        type: "PATCH",
                        content_type: "application/json",
                        timeout: 10000,
                        success: function (xhr) {
                            dom_pw.text(value);
                        }
                    });
                }
            },
            allowOutsideClick: () => !Swal.isLoading()
        }).then(
            (result) => {
                let res = result.value;
                if (res && res.code === 200){
                    show_warning_dialog(getTransText("修改密码成功"), "success");
                }
            },
            (error) => {
                if(error.statusText === "timeout"){
                    show_warning_dialog("timeout");
                }else{
                    let msg = get_err_msg_or_default(error, getTransText("修改密码失败") + ';');
                    show_warning_dialog(msg, "error");
                }
            }
        )
    }

    // FTP密码修改事件
    $("#content-display-div").on("click", '.ftp-password-edit', function (e) {
        e.preventDefault();
        let dom_pw = $(this).prev();
        changeFtpPassword(dom_pw, 'password');
    });

    // FTP只读密码双击修改事件
    $("#content-display-div").on("click", '.ftp-ro-password-edit', function (e) {
        e.preventDefault();
        let dom_pw = $(this).prev();
        changeFtpPassword(dom_pw, 'ro_password');
    });

    //  同步请求使能ftp或修改FTP密码
    //@ return:
    //      success: true
    //      failed : false
    function ftp_enable_password_ajax(url) {
        let ret = {ok:false, msg:''};

        $.ajax({
            url: url,
            type: "PATCH",
            content_type: "application/json",
            timeout: 5000,
            async: false,
            success: function (res) {
                if(res.code === 200){
                    ret.ok = true;
                }
            },
            error: function(xhr){
                ret.msg = get_err_msg_or_default(xhr, getTransText('请求失败') + '!');
            },
            complete : function(xhr,status){
                if (status === 'timeout') {// 判断超时后 执行
                    alert("timeout");
                }
            },
        });

        return ret;
    }

    // FTP开启或关闭点击修改事件
    $("#content-display-div").on("click", '.ftp-enable-btn', function (e) {
        e.preventDefault();

        let data = {};
        data.bucket_name = $(this).attr('data-bucket-name');
        let status_node = $(this).prev();
        (async function() {
            const {value: result} = await Swal.fire({
                title: getTransText('开启或关闭FTP'),
                input: 'radio',
                inputOptions: {
                    true: getTransText('开启'),
                    false: getTransText('关闭'),
                },
                showCancelButton: true,
                inputValidator: (value) => {
                    return !value && 'You need to choose something!'
                }
            });

            let enable = false;
            if (result === 'true'){
                enable = true;
            }else if (result === 'false'){
                enable = false;
            }else{
                return;
            }
            data.enable = enable;
            let url = build_ftp_patch_url(data);
            let ret = ftp_enable_password_ajax(url);
            if(ret.ok){
                if (enable){
                    status_node.html(getTransText("开启"));
                }else{
                    status_node.html(getTransText("关闭"));
                }
                show_warning_dialog(getTransText("配置存储桶FTP成功"), "success");
            }else{
                show_warning_dialog(getTransText('配置存储桶FTP失败') + ret.msg, 'error');
            }
        })();
    });

    // 分享存储桶
    $("#content-display-div").on("click", '.btn-share-bucket', function (e) {
        e.preventDefault();

        let status_node = $(this).prev();
        let bucket_item = $(this).parents("#bucket-list-item");
        let td = bucket_item.children('td:first-child');
        let check = td.children(':checkbox:first-child');
        let b_id = check.val();
        let op = '1';

        Swal.fire({
            title: getTransText('选择权限'),
            input: 'radio',
            inputOptions: {
                '1': getTransText('公开'),
                '2': getTransText('私有'),
            },
            showCancelButton: true,
            inputValidator: (value) => {
                return !value && 'You need to choose something!'
            },
            inputAttributes: {
                autocapitalize: 'off'
            },
            confirmButtonText: getTransText('确定'),
            showLoaderOnConfirm: true,
            preConfirm: (value) => {
                op = value;
                return $.ajax({
                    url: build_buckets_permission_url({id: b_id, public: value}),
                    type: 'patch',
                    async: true,
                    success: function (data, status_text, xhr) {
                        if (op === "1") {
                            status_node.html(getTransText('公有'));
                        } else {
                            status_node.html(getTransText('私有'));
                        }
                        return data;
                    },
                    error: function (xhr, msg, err) {
                        Swal.showValidationMessage('Request failed:' + xhr.statusText);
                    },
                });
            },
            allowOutsideClick: () => !Swal.isLoading()
        }).then((result) => {
            if (result.value && (op === "1")) {
                Swal.fire({
                    title: getTransText("分享链接"),
                    text: result.value.share[0]
                });
            }
        })
    });


    //
    //存储桶文件列表视图渲染模板
    //
    let render_bucket_files_view = template.compile(`
        <div class="container-fluid">
            <div class="row">
                <div class="col-12">
                    <nav aria-label="breadcrumb">
                        <ol class="breadcrumb">
                            <a href="#" id="btn-path-bucket">{{$imports.getTransText('存储桶')}}</a>
                            <span>></span>
                            <li class="breadcrumb-item"><a href="#" id="btn-path-item" bucket_name="{{ $data['bucket_name']}}"  dir_path="">{{ $data['bucket_name']}}</a></li>
                            {{set breadcrumbs = $imports.get_breadcrumb($data['dir_path'])}}
                            {{ each breadcrumbs }}
                                <li class="breadcrumb-item"><a href="#"  id="btn-path-item" bucket_name="{{ $data['bucket_name']}}"  dir_path="{{$value[1]}}">{{ $value[0] }}</a></li>
                            {{/each}}
                        </ol>
                    </nav>
                    <hr>
                    <div>
                        <button class="btn btn-info" id="btn-new-directory"><i class="fa fa-plus"></i>{{$imports.getTransText('创建文件夹')}}</button>
                        <button class="btn btn-primary" id="btn-upload-file" bucket_name="{{ $data['bucket_name'] }}" cur_dir_path="{{ $data['dir_path'] }}"><i class="fa fa-upload"></i>{{$imports.getTransText('上传文件')}}</button>
                        <button class="btn btn-success" id="btn-path-item" bucket_name="{{ $data['bucket_name'] }}" dir_path="{{ $data['dir_path'] }}"><i class="fa fa-sync"></i></button>
                        <div  id="upload-progress-bar" style="display: none;">
                            <p class="text-warning">{{$imports.getTransText('请勿离开此页面，以防文件上传过程中断！')}}</p>
                            <div class="progress text-warning">             
                                <div class="progress-bar"  role="progressbar" aria-valuenow="0" aria-valuemin="0"
                                     aria-valuemax="100" style="min-width: 2em;width: 0%;">
                                    0%
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            <hr style=" height:1px;border:1px;border-top:1px solid #185598;"/>
            <div class="row">
                <div class="container-fluid">
                    <table class="table table-hover" id="bucket-files-table">
                        <thead class="thead-light">
                        <tr class="bg-light">
                            <th><input type="checkbox" data-check-target=".item-checkbox" /></th>
                            <th>{{$imports.getTransText('名称')}}</th>
                            <th>{{$imports.getTransText('上传时间')}}</th>
                            <th>{{$imports.getTransText('大小')}}</th>
                            <th>{{$imports.getTransText('权限')}}</th>
                            <th></th>
                        </tr>
                        </thead>
                        {{set str_operation = $imports.getTransText('操作')}}
                        {{set str_open = $imports.getTransText('打开')}}
                        {{set str_delete = $imports.getTransText('删除')}}
                        {{set str_share = $imports.getTransText('分享公开')}}
                        {{set str_download = $imports.getTransText('下载')}}
                        {{set str_rename = $imports.getTransText('重命名')}}
                        {{each files}}
                        <tbody>
                            <tr class="bucket-files-table-item">
                                <td><input type="checkbox" class="item-checkbox" value=""></td>
                                <!--文件-->
                                {{ if $value.fod }}
                                    <td>
                                        <i class="fa fa-file"></i>
                                        <a href="#" id="bucket-files-item-enter-file" download_url="{{$value.download_url}}">{{ $value.name }}</a>
                                    </td>
                                    <td>{{ $imports.isoTimeToLocal($value.ult) }}</td>
                                    <td>{{ $imports.sizeFormat($value.si, "B") }}</td>
                                {{/if}}
                                {{ if !$value.fod }}
                                    <td>
                                        <i class="fa fa-folder"></i>
                                        <a href="#" id="bucket-files-item-enter-dir" dir_path="{{$value.na}}"><strong>{{ $value.name }}</strong></a>
                                    </td>
                                    <td>{{ $imports.isoTimeToLocal($value.ult) }}</td>
                                    <td>--</td>
                                {{/if}}
                                <td id="id-access-perms">{{ $value.access_permission}}</td>
                                <td>
                                    <div class="dropdown">
                                        <button type="button" class="dropdown-toggle btn btn-outline-info" data-toggle="dropdown" role="button" aria-haspopup="true"
                                   aria-expanded="false">{{str_operation}}<span class="caret"></span></button>
                                        <div class="dropdown-menu">
                                            <!--目录-->
                                            {{ if !$value.fod }}
                                                <a class="dropdown-item bg-info" href="" id="bucket-files-item-enter-dir" dir_path="{{$value.na}}">{{str_open}}</a>
                                                <a class="dropdown-item bg-danger" href="" id="bucket-files-item-delete-dir" dir_path="{{$value.na}}">{{str_delete}}</a>
                                                <a class="dropdown-item bg-warning" href="#" id="bucket-files-item-dir-share" dir_path="{{$value.na}}" data-access-code="{{$value.access_code}}">{{str_share}}</a>
                                            {{/if}}
                                            <!--文件-->
                                            {{ if $value.fod }}
                                                <a class="dropdown-item bg-success" id="bucket-files-item-download" href="{{$value.download_url}}" >{{str_download}}</a>
                                                <a class="dropdown-item bg-danger" id="bucket-files-item-delete" href="" filename="{{$value.name}}">{{str_delete}}</a>
                                                <a class="dropdown-item bg-info" id="bucket-files-obj-share" href="" bucket_name="{{ $data['bucket_name']}}"  dir_path="{{$data['dir_path']}}" filename="{{$value.name}}" data-access-code="{{$value.access_code}}">{{str_share}}</a>
                                                <a class="dropdown-item bg-warning" id="bucket-files-obj-rename" href="" bucket_name="{{ $data['bucket_name']}}"  dir_path="{{$data['dir_path']}}" filename="{{$value.name}}">{{str_rename}}</a>
                                        {{/if}}
                                        </div>
                                    </div>
                                </td>
                            </tr>
                        </tbody>
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


    //
    // 单个存储桶文件列表单个文件对象项渲染模板
    //
    let render_bucket_file_item = template.compile(`
        <tr class="bucket-files-table-item">
            <td><input type="checkbox" class="item-checkbox" value=""></td>
            <td>
                <i class="fa fa-file"></i>
                <a href="#" id="bucket-files-item-enter-file"  download_url="{{obj.download_url}}">{{ obj.name }}</a>
            </td>
            <td>{{ $imports.isoTimeToLocal(obj.ult) }}</td>
            <td>{{ $imports.sizeFormat(obj.si, "B") }}</td>
            <td id="id-access-perms">{{ obj.access_permission }}</td>
            <td>
                <div class="dropdown">
                    <button type="button" class="dropdown-toggle btn btn-outline-info" data-toggle="dropdown" role="button" aria-haspopup="true"
               aria-expanded="false">{{$imports.getTransText('操作')}}<span class="caret"></span></button>
                    <div class="dropdown-menu">
                        <a class="dropdown-item bg-success" id="bucket-files-item-download" href="{{obj.download_url}}" >{{$imports.getTransText('下载')}}</a>
                        <a class="dropdown-item bg-danger" id="bucket-files-item-delete" href="" filename="{{obj.name}}">{{$imports.getTransText('删除')}}</a>
                        <a class="dropdown-item bg-info" id="bucket-files-obj-share" href="" bucket_name="{{ $data['bucket_name']}}"  dir_path="{{$data['dir_path']}}" filename="{{obj.name}}"  data-access-code="{{obj.access_code}}">{{$imports.getTransText('分享公开')}}</a>
                        <a class="dropdown-item bg-warning" id="bucket-files-obj-rename" href="" bucket_name="{{ $data['bucket_name']}}"  dir_path="{{$data['dir_path']}}" filename="{{obj.name}}">{{$imports.getTransText('重命名')}}</a>
                    </div>
                </div>
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
        let bucket_name = $(this).attr('bucket_name');
        let dir_path = $(this).attr('dir_path');

        let url = build_dir_detail_url({
            bucket_name: bucket_name,
            dir_path: dir_path
        });
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
            show_auto_close_warning_dialog(getTransText("请输入一个有效的正整数页码"));
            return;
        }

        let bucket_name = $(this).attr('data-bucket_name');
        let dir_path = $(this).attr('data-dir_path');

        let url = build_dir_detail_url({
            bucket_name: bucket_name,
            dir_path: dir_path
        });

        let limit = 200;
        let offset = (page_num - 1) * limit;
        let q = encode_params({offset: offset, limit: limit});
        url = url + "?" + q;
        get_bucket_files_and_render(url);
    });

    //
    // 存储桶文件删除事件处理
    //
    $("#content-display-div").on("click", '#bucket-files-item-delete', function (e) {
        e.preventDefault();

        let list_item_dom = $(this).parents("tr.bucket-files-table-item");
        let filename = $(this).attr("filename");
        let obj = get_bucket_name_and_cur_path();
        obj.filename = filename;

        let url = build_obj_detail_url(obj);

        show_confirm_dialog({
            title: getTransText('确定要删除吗？'),
            ok_todo: function() {
                delete_one_file(url, function () {
                    list_item_dom.remove();
                })
            },
        });
    });


    //
    // 文件重命名事件处理
    //
    $("#content-display-div").on("click", '#bucket-files-obj-rename', function (e) {
        e.preventDefault();

        let list_item_dom = $(this).parents("tr.bucket-files-table-item");
        let btn_dom = $(this);
        let filename = btn_dom.attr("filename");
        let bucket_name = btn_dom.attr('bucket_name');
        let dir_path = btn_dom.attr('dir_path');
        let paths=[bucket_name, dir_path, filename];

        Swal.fire({
            title: getTransText('请修改对象名称'),
            input: 'text',
            inputValue: filename,
            inputAutoTrim: true,
            inputAttributes: {
                autocapitalize: 'off'
            },
            showCancelButton: true,
            confirmButtonText: getTransText('确定'),
            cancelButtonText: getTransText('取消'),
            showLoaderOnConfirm: true,
            inputValidator: (value) => {
                if (value==='')
                    return !value && getTransText('请输入一些内容, 前后空格会自动去除');
                else if (value.length > 255)
                    return getTransText("对象名长度不能大于255字符");
                else if(value === filename){
                    return getTransText("对象名未修改");
                }
                return !(value.indexOf('/') === -1) && getTransText('对象名不能包含‘/’');
              },
            preConfirm: (input_name) => {
                let url = build_move_rename_url(paths=[bucket_name, dir_path, filename], {rename: input_name});
                return $.ajax({
                    url: url,
                    type: 'post',
                    data: {},
                    timeout: 20000,
                    success: (result) => {
                        if (result.code === 201){
                            return result;
                        }else{
                            Swal.showValidationMessage(
                            `Request failed: ${result.code_text}`
                            );
                        }
                    },
                    headers: {'X-Requested-With': 'XMLHttpRequest'},//判断是否是异步请求时需要此响应头
                });
            },
            allowOutsideClick: () => !Swal.isLoading()
        }).then(
            (result) => {
                if (result.value) {
                    let html = render_bucket_file_item(result.value);
                    list_item_dom.after(html);
                    list_item_dom.remove();
                    show_warning_dialog(getTransText('重命名成功'), 'success');
                }
             },
            (error) => {
                let msg;
                try{
                    msg = error.responseJSON.code_text;
                }
                catch (e) {
                    msg = error.statusText;
                }
                show_warning_dialog(getTransText('重命名失败')+ msg);
            }
        )
    });

    let render_share_dialog_select = template.compile(`
        <select id="swal-select" class="swal2-select">
            {{set str_day = $imports.getTransText('天')}}
            <option value="0">{{$imports.getTransText('永久公开')}}</option>
            <option value="1">1{{str_day}}</option>
            <option value="7">7{{str_day}}</option>
            <option value="30">30{{str_day}}</option>
            <option value="-1">{{$imports.getTransText('私有')}}</option>
        </select>
        <div>
            <input type="checkbox" id="swal-password" class="swal2-checkbox" ><span>{{$imports.getTransText('有分享密码保护')}}</span>
        </div> `);
    //
    //
    //@param obj: {
    //             bucket_name: "xxx",
    //             dir_path: "xxx"
    //         }
    function bucket_dir_share(obj, click_dom){
        let url = build_dir_detail_url(obj);
        let status_node = click_dom.parents("tr.bucket-files-table-item").find("td#id-access-perms");
        let select_html = render_share_dialog_select({});
        let share = 1;
        Swal.fire({
            title: getTransText('分享'),
            html: select_html,
            showCancelButton: true,
            cancelButtonText: getTransText('取消'),
            inputAttributes: {
                autocapitalize: 'off'
            },
            confirmButtonText: getTransText('确定'),
            showLoaderOnConfirm: true,
            showCloseButton: true,
            footer: getTransText('提示：创建新的带密码的分享，旧的分享密码会失效'),
            preConfirm: () => {
                let value = document.getElementById('swal-select').value;
                let is_pw = document.getElementById('swal-password').checked;
                if (value === '-1'){
                    share = 0;
                }

                let params = {share: share, days: value};
                if (is_pw)
                    params.password = '';
                url = url + '?' + $.param(params, true);

                return $.ajax({
                    url: url,
                    type: 'patch',
                    async: true,
                    success: function (data, status_text, xhr) {
                        if (data.hasOwnProperty('access_code') && data.access_code !== 0) {
                            let s = getTransText('公有');
                            click_dom.attr('data-access-code', data.access_code);
                            status_node.html(s);
                        } else {
                            let s = getTransText('私有');
                            click_dom.attr('data-access-code', data.access_code);
                            status_node.html(s);
                        }
                        return data;
                    }
                });
            },
            allowOutsideClick: () => !Swal.isLoading()
        }).then((result) => {
            if (result.value && (share === 1)) {
                let text = '<p>' + result.value.share + '</p>';
                let pw = result.value.share_code;
                if (pw)
                    text = text + '<p>' + getTransText('分享密码') + ':' + pw + '</p>';
                Swal.fire({
                    title: getTransText("分享链接"),
                    html: text
                });
            }
        }).catch((xhr) => {
            let msg = getTransText('分享公开设置失败')+ xhr.statusText;
            msg = get_err_msg_or_default(xhr, msg);
            show_warning_dialog(msg,'error');
        })
    }

    // 目录文件夹分享公开点击事件处理
    $("#content-display-div").on("click", '#bucket-files-item-dir-share', function (e) {
        e.preventDefault();

        let bucket_name = get_bucket_name_and_cur_path().bucket_name;
        let dir_path = $(this).attr('dir_path');
        let obj = {
            bucket_name: bucket_name,
            dir_path: dir_path
        };

        let aCode = $(this).attr("data-access-code");
        if (aCode === '0'){
            bucket_dir_share(obj, $(this));
            return;
        }

        Swal.fire({
            title: getTransText("目录已是公共可访问的，或者已经设置过分享，请选择是从新设置分享，还是查询现有的旧的分享连接。"),
            showDenyButton: true,
            showCancelButton: true,
            confirmButtonText: getTransText("公开分享"),
            denyButtonText: getTransText("查询"),
            cancelButtonText: getTransText("取消"),
            footer: getTransText('提示：创建新的带密码的分享，旧的分享链接会失效')
        }).then((result) => {
            if (result.isConfirmed) {
                bucket_dir_share(obj, $(this));
            } else if (result.isDenied) {
                let params = {
                    bucket_name: obj.bucket_name,
                    path: obj.dir_path
                };
                get_share_uri(params);
            }
        });

    });


    //
    //@param obj: {
    //             bucket_name: "xxx",
    //             dir_path: "xxx",
    //             filename: "xxx"
    //         }
    function bucket_object_share(obj, click_dom){
        let status_node = click_dom.parents("tr.bucket-files-table-item").find("td#id-access-perms");
        let detail_url = build_obj_detail_url(obj);
        let select_html = render_share_dialog_select({});
        let share = 1;
        Swal.fire({
            title: getTransText('分享'),
            html: select_html,
            showCancelButton: true,
            cancelButtonText: getTransText('取消'),
            inputAttributes: {
                autocapitalize: 'off'
            },
            confirmButtonText: getTransText('确定'),
            showLoaderOnConfirm: true,
            showCloseButton: true,
            footer: getTransText('提示：创建新的带密码的分享，旧的分享链接会失效'),
            preConfirm: () => {
                let value = document.getElementById('swal-select').value;
                let is_pw = document.getElementById('swal-password').checked;
                if (value === '-1'){
                    share = 0;
                }

                let params = {share: share, days: value};
                if (is_pw)
                    params.password = '';
                let url = detail_url + '?' + $.param(params, true);
                return $.ajax({
                    url: url,
                    type: 'patch',
                    async: true,
                    success: function (data, status_text, xhr) {
                        if (data.hasOwnProperty('access_code') && data.access_code !== 0) {
                            let s = getTransText('公有');
                            click_dom.attr('data-access-code', data.access_code);
                            status_node.html(s);
                        } else {
                            let s = getTransText('私有');
                            click_dom.attr('data-access-code', data.access_code);
                            status_node.html(s);
                        }
                        return data;
                    }
                });
            },
            allowOutsideClick: () => !Swal.isLoading()
        }).then((result) => {
            if (result.value && (share === 1)) {
                Swal.fire({
                    title: getTransText("分享链接"),
                    text: result.value.share_uri
                });
            }
        }).catch((xhr) => {
            let msg = getTransText('分享公开设置失败！')+ xhr.statusText;
            msg = get_err_msg_or_default(xhr, msg);
            show_warning_dialog(msg, 'error');
        })
    }

    //查询分享连接
    //@param obj: {
    //             bucket_name: "xxx",
    //             path: "xxx"
    //         }
    function get_share_uri(obj){
        let url = build_share_detail_url(obj);
        Swal.showLoading();
        $.ajax({
            url: url,
            timeout: 20000,
            success: function(data,status,xhr){
                Swal.close();
                if(xhr.hasOwnProperty('responseJSON') && xhr.responseJSON.hasOwnProperty('share_uri')) {
                    let share_uri = xhr.responseJSON.share_uri;
                    if(xhr.responseJSON.is_obj){
                        Swal.fire({
                            title: getTransText("分享链接"),
                            text: share_uri
                        });
                        return;
                    }
                    let text = '<p>' + share_uri + '</p>';
                    if (xhr.responseJSON.hasOwnProperty('share_code')){
                        let share_code = xhr.responseJSON.share_code;
                        text = text + '<p>' + getTransText('分享密码') + ':' + share_code + '</p>';
                    }
                    Swal.fire({
                        title: getTransText("分享链接"),
                        html: text
                    });
                }
            },
            error: function (xhr, errtype, error) {
                Swal.close();
                if (errtype === 'timeout'){
                    show_warning_dialog(getTransText('请求超时'), 'error');
                    return;
                }
                if(xhr.hasOwnProperty('responseJSON') && xhr.responseJSON.hasOwnProperty('code')){
                    let code = xhr.responseJSON.code;
                    if(code === 'NotShared'){
                        show_warning_dialog(getTransText('对象或目录未共享或者共享时间到期，请刷新后重试。'), 'error');
                    }else if (code === 'NoSuchBucket'){
                        show_warning_dialog(getTransText('存储桶不存在，请刷新页面。'), 'error');
                    }else if (code === 'NoSuchKey'){
                        show_warning_dialog(getTransText('对象或目录不存在，请刷新后重试。'), 'error');
                    }else{
                        show_warning_dialog(getTransText(xhr.responseJSON.message), 'error');
                    }
                }else{
                    show_warning_dialog(getTransText('请求失败，请刷新后重试。'), 'error');
                }
            }
        });
    }

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

        let aCode = $(this).attr("data-access-code");
        if (aCode === '0'){
            bucket_object_share(obj, $(this));
            return;
        }
        Swal.fire({
            title: getTransText("对象已是公共可访问的，或者已经设置过分享，请选择是从新设置分享，还是查询现有的旧的分享连接。"),
            showDenyButton: true,
            showCancelButton: true,
            confirmButtonText: getTransText("公开分享"),
            denyButtonText: getTransText("查询"),
            cancelButtonText: getTransText("取消"),
            footer: getTransText('提示：创建新的带密码的分享，旧的分享链接会失效')
        }).then((result) => {
            if (result.isConfirmed) {
                bucket_object_share(obj, $(this));
            } else if (result.isDenied) {
                let params = {
                    bucket_name: obj.bucket_name,
                    path: obj.dir_path + '/' + obj.filename
                };
                get_share_uri(params);
            }
        });
    });


    //
    // 删除一个文件
    //
    function delete_one_file(url, success_do){
        Swal.showLoading();
        $.ajax({
            type: 'delete',
            url: url,
            success: function(data,status,xhr){
                Swal.close();
                success_do();
                show_auto_close_warning_dialog(getTransText('删除成功'), type='success');
            },
            error: function (error) {
                Swal.close();
                show_warning_dialog(getTransText('删除失败'), type='error');
            }
        });
    }


    //
    // 删除文件夹事件处理
    //
    $("#content-display-div").on("click", '#bucket-files-item-delete-dir', function (e) {
        e.preventDefault();
        let list_item_dom = $(this).parents("tr.bucket-files-table-item");
        let bucket_name = get_bucket_name_and_cur_path().bucket_name;
        let dir_path = $(this).attr('dir_path');

        let url = build_dir_detail_url({
            bucket_name: bucket_name,
            dir_path: dir_path
        });

        show_confirm_dialog({
            title: getTransText('确定要删除吗？'),
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
        Swal.showLoading();
        $.ajax({
            type: 'delete',
            url: url,
            success: function(data,status,xhr){
                Swal.close();
                success_do();
                show_auto_close_warning_dialog(getTransText('删除成功'), 'success');
            },
            error: function (error,status) {
                Swal.close();
                let msg = getTransText('删除失败')+ error.statusText;
                msg = get_err_msg_or_default(error, msg);
                show_warning_dialog(msg, 'error');
            }
        });
    }


    //
    // 存储桶列表项点击进入事件处理
    //
    $("#content-display-div").on("click", '#bucket-list-item-enter', function (e) {
        e.preventDefault();
        let bucket_name = $(this).attr('bucket_name');

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
        let url = build_metadata_api(obj);
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
        Swal.showLoading();
        $.ajax({
            url: url,
            data: data,
            timeout: 20000,
            success: function(data,status,xhr){
                Swal.close();
                if (status === 'success'){
                    let html = render(data);
                    $content_display_div.empty();
                    $content_display_div.append(html);
                }else{
                    show_warning_dialog(getTransText('好像出问题了，跑丢了') + '( T__T ) …', 'error');
                }
            },
            error: function (xhr, errtype, error) {
                Swal.close();
                if (errtype === 'timeout'){
                    show_warning_dialog(getTransText('请求超时'), 'error');
                }else{
                    show_warning_dialog(getTransText('好像出问题了，跑丢了') + '( T__T ) …', 'error');
                }
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
                <nav aria-label="breadcrumb">
                    <ol class="breadcrumb">
                        <a href="#" id="btn-path-bucket">{{$imports.getTransText('存储桶')}}</a>
                        <span>></span>
                        <li class="breadcrumb-item"><a href="#" id="btn-path-item" bucket_name="{{ $data['bucket_name']}}"  dir_path="">{{ $data['bucket_name']}}</a></li>
                        {{set breadcrumbs = $imports.get_breadcrumb($data['dir_path'])}}
                        {{each breadcrumbs}}
                            <li class="breadcrumb-item"><a href="#"  id="btn-path-item" bucket_name="{{ $data['bucket_name']}}"  dir_path="{{$value[1]}}">{{ $value[0] }}</a></li>
                        {{/each}}
                    </ol>
                    <p><h3>{{ obj.name }}</h3></p>
                </nav>
            </div>
            <div class="col-sm-12"><hr style=" height:1px;border:1px;border-top:1px solid #185598;" /></div>
            <div class="col-xs-12 col-sm-12">
                <div>
                    <a class="btn btn-info" href="{{ obj.download_url }}">{{$imports.getTransText('下载')}}</a>
                </div>
                <hr/>
                <div>
                    <strong>{{$imports.getTransText('对象名称')}}：</strong>
                    <p>{{ obj.name }}</p>
                    <p><strong>{{$imports.getTransText('对象大小')}}：</strong>{{ obj.si }}</p>                   
                    <p><strong>{{$imports.getTransText('创建日期')}}：</strong>{{ $imports.isoTimeToLocal(obj.ult) }}</p>                   
                    <p><strong>{{$imports.getTransText('修改日期')}}：</strong>{{ $imports.isoTimeToLocal(obj.upt) }}</p> 
                    <p><strong>{{$imports.getTransText('下载次数')}}：</strong>                
                    {{if obj.dlc}}
                        {{ obj.dlc }}
                    {{else if !obj.dlc}}
                        0
                    {{/if}}
                    </p>                    
                    <p><strong>{{$imports.getTransText('访问权限')}}：</strong>{{obj.access_permission}}</p>
                    {{if obj.sh}}
                        <p><strong>分享终止时间：</strong>
                        {{if obj.stl}}
                            {{$imports.isoTimeToLocal(obj.set)}}
                        {{else if !obj.stl}}
                            {{$imports.getTransText('永久公开')}}
                        {{/if}}
                        </p>
                    {{/if}}
                    <strong>{{$imports.getTransText('下载连接')}}：</strong>
                    <p><a href="{{ obj.download_url }}" class="text-auto-break">{{ obj.download_url }}</a></p>
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

            const {value: file} = await Swal.fire({
                title: getTransText('选择文件'),
                input: 'file',
                showCancelButton: true,
                cancelButtonText: getTransText('取消'),
                confirmButtonText: getTransText('确定'),
                inputAttributes: {
                    'accept': '*',
                    'aria-label': 'Upload your select file'
                }
            });
            if (file) {
                uploadOneFile(file);//上传文件
            }
            else if(file === null){
                show_warning_dialog(getTransText("没有选择文件，请先选择一个文件"));
            }
        }
    );

    //
    // 上传一个文件
    //
    function uploadOneFile(file) {
        if(file.size === 0) {
            show_warning_dialog(getTransText("无法上传一个空文件"));
            return;
        }
        if(file.size >= 5*1024**3) {
            show_warning_dialog(getTransText("文件太大"));
            return;
        }
        let obj = get_bucket_name_and_cur_path();
        if(!obj.bucket_name){
            show_warning_dialog(getTransText('上传文件失败，无法获取当前存储桶下路径'));
            return;
        }

        let url = build_obj_detail_url({
            bucket_name: obj.bucket_name,
            dir_path: obj.dir_path,
            filename: file.name
        });
        ajaxUploadOneFile(url, file);
    }

    // 一次上传完整文件
    function ajaxUploadOneFile(url, file) {
        let formData = new FormData();
        formData.append("file", file);

        $.ajax({
            url: url,
            type: "PUT",
            data: formData,
            contentType: false,
            processData: false,
            xhr: function(){
    　　　　　　let xhr = $.ajaxSettings.xhr();
    　　　　　　if(xhr.upload) {
    　　　　　　　　xhr.upload.addEventListener("progress" , function(e){
                        fileUploadProgressBar(e.loaded, e.total);
                    }, false);
    　　　　　　　　return xhr;
    　　　　　　}
    　　　　},
            beforeSend: function(xhr, contents){
                let csrftoken = getCookie('csrftoken');
                xhr.setRequestHeader("X-CSRFToken", csrftoken);
                beforeFileUploading();
            },
            success: function (data) {
                show_auto_close_warning_dialog(getTransText('文件已成功上传'), 'success', 'top-end');
                try{
                    if (data.created === true){
                        // 如果上传的文件在当前页面的列表中，插入文件列表
                        success_upload_file_append_list_item(url, file.name);
                    }
                }catch (e) {}
            },
            error: function(xhr, textStatus, errorType){
                let msg = getTransText('上传文件发生错误,请重新上传');
                if (xhr.responseJSON && xhr.responseJSON.hasOwnProperty('code_text'))
                    show_warning_dialog(msg + xhr.responseJSON.code_text);
                else
                    show_warning_dialog(msg + xhr.statusText);
            },
            complete: function () {
                endFileUploading();
            }
        })
    }


    //
    // 成功上传文件后，插入文件列表项
    function success_upload_file_append_list_item(obj_url, filename) {
        let obj = get_bucket_name_and_cur_path();
        if(!obj.bucket_name){
            return;
        }

        let params = {
            bucket_name: obj.bucket_name,
            dir_path: obj.dir_path,
            filename: filename
        };
        let cur_url = build_obj_detail_url(params);

        if (cur_url === obj_url){
            let info_url = build_url_with_domain_name(build_metadata_api(params));
            get_file_info_and_list_item_render(info_url, render_bucket_file_item);
        }
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
        let percent = width + '%';
        $bar.find("div.progress-bar").attr({"style": "min-width: 2em;width: " + percent + ";"});
        $bar.find("div.progress-bar").text(percent);
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
        Swal.fire({
            title: getTransText('请输入一个文件夹名称'),
            input: 'text',
            inputAutoTrim: true,
            inputAttributes: {
                autocapitalize: 'off'
            },
            showCancelButton: true,
            confirmButtonText: getTransText('确定'),
            cancelButtonText: getTransText('取消'),
            showLoaderOnConfirm: true,
            inputValidator: (value) => {
                if (value==='')
                    return !value && getTransText('请输入一些内容, 前后空格会自动去除');
                return !(value.indexOf('/') === -1) && getTransText('目录名不能包含‘/’')
              },
            preConfirm: (input_name) => {
                let obj = get_bucket_name_and_cur_path();
                obj.dir_name = input_name;

                return $.ajax({
                    url: build_dir_detail_url(obj),
                    type: 'post',
                    data: {},
                    timeout: 20000,
                    // contentType: false,//必须false才会自动加上正确的Content-Type
                    // processData: false,//必须false才会避开jQuery对 formdata 的默认处理,XMLHttpRequest会对 formdata 进行正确的处理
                    success: (result) => {
                        if (result.code === 201){
                            return result;
                        }else{
                            Swal.showValidationMessage(
                            `Request failed: ${result.code_text}`
                            );
                        }
                    },
                    headers: {'X-Requested-With': 'XMLHttpRequest'},//django判断是否是异步请求时需要此响应头
                    clearForm: false,//禁止清除表单
                    resetForm: false //禁止重置表单
                });
            },
            allowOutsideClick: () => !Swal.isLoading()
        }).then(
            (result) => {
                if (result.value) {
                    let html = render_bucket_dir_item(result.value);
                    $("#bucket-files-table tr:eq(0)").after(html);
                    show_warning_dialog(transInterpolate(getTransText('创建文件夹“%s”成功'), result.value.data.dir_name), 'success');
                }
             },
            (error) => {
                let msg;
                try{
                    msg = error.responseJSON.code_text;
                }
                catch (e) {
                    msg = error.statusText;
                }

                show_warning_dialog(getTransText('创建失败') + msg);
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
                <i class="fa fa-folder"></i>
                <a href="#" id="bucket-files-item-enter-dir" dir_path="{{dir.na}}"><strong>{{ dir.name }}</strong></a>
            </td>
            <td>{{ $imports.isoTimeToLocal(dir.ult) }}</td>
            <td>--</td>
            <td id="id-access-perms">{{ dir.access_permission}}</td>
            <td>
                <div class="dropdown">
                    <button class="dropdown-toggle btn btn-outline-info" data-toggle="dropdown" role="button" aria-haspopup="true"
               aria-expanded="false">{{$imports.getTransText('操作')}}<span class="caret"></span></button>
                    <div class="dropdown-menu">
                         <a class="dropdown-item bg-info" id="bucket-files-item-enter-dir" dir_path="{{dir.na}}">{{$imports.getTransText('打开')}}</a>
                         <a class="dropdown-item bg-danger" id="bucket-files-item-delete-dir" dir_path="{{dir.na}}">{{$imports.getTransText('删除')}}</a>
                         <a class="dropdown-item bg-warning" id="bucket-files-item-dir-share" dir_path="{{dir.na}}" data-access-code="{{dir.access_code}}">{{$imports.getTransText('分享公开')}}</a>
                    </div>
                </div>
            </td>
        </tr>
    `);

})();



