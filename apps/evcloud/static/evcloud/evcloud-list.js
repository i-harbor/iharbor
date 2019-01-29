;(function() {
    var process = '<div class="progress">' +
                        '<div class="progress-bar progress-bar-striped active" role="progressbar" aria-valuenow="45" aria-valuemin="0" aria-valuemax="100" style="width: 100%">' +
                            '<span class="sr-only">45% Complete</span>' +
                        '</div>' +
                    '</div>';

    $('#btn-evcloud-add').click(function () {
        self.location.href = "/evcloud/add/";
    });
    //获取状态，并且显示
    function get_status(vm_id, click_element) {
        $.ajax({
            url: '/evcloud/list/',
            type: "POST",
            data: {
                "vm_id": vm_id,
                "vm_operate": '6',
                "csrfmiddlewaretoken": $('[name="csrfmiddlewaretoken"]').val(),
            },
            datatype: "json",
            success: function (data) {
                if(data.code === 200){
                    click_element.children('[name="status"]').html(data.e);
                    if(data.e == '关机'){
                        click_element.find('[name="vnc"]').addClass('disabled');
                    }
                    if(data.e == '运行'){
                        click_element.find('[name="vnc"]').removeClass('disabled');
                    }
                    //click_element.children('[name="status"]').addClass('btn-danger');
                    //click_element.parents('td').siblings('[name="mission"]').html('');
                } else {
                    click_element.children('[name="status"]').html('failed');
                    click_element.children('[name="operate"]').addClass('disabled');
                    swal({
                        type: 'error',
                        title: '查询状态失败',
                        text: data.e,
                    })
                }
            },
            error: function () {
                click_element.children('[name="status"]').html('暂停服务');
                swal({
                        type: 'error',
                        title: '暂停服务',
                        text: '当前服务不可用',
                    })
                return 0;
            },
            headers: {'X-Requested-With': 'XMLHttpRequest'},
        })
    }
    //查询虚拟机状态
    $('.vm-line').each(function () {
        get_status(this.id, $(this))
    });

    //启动、关闭、断电、重启、删除
    $('.vm-operate').click(function (event) {
        event.preventDefault();
        let operate_list = ['启动', '关闭', '断电', '重启' ,'删除'];
        click_element = $(this);
        vm_operate = event.target.getAttribute('value');
        vm_id = click_element.parents('tr').attr('id');
        click_element.parents('td').siblings('[name="status"]').html(process);
        click_element.parents('td').siblings('[name="mission"]').html(operate_list[vm_operate]);
        $.ajax({
            url: "/evcloud/list/",
            type: "POST",
            data: {
                "vm_id": vm_id,
                "vm_operate": vm_operate,
                "csrfmiddlewaretoken": $('[name="csrfmiddlewaretoken"]').val(),
            },
            datatype: "json",
            success: function (data) {
                if(data.code === 200) {
                    if (data.status === 'delete') {
                        $("#" + vm_id).remove();
                        return;
                    }
                    get_status(vm_id, click_element.parents('tr'));
                    //click_element.parents('td').siblings('[name="status"]').html(data.status);
                    click_element.parents('td').siblings('[name="mission"]').html('');
                } else {
                    get_status(vm_id, click_element.parents('tr'));
                    click_element.parents('td').siblings('[name="mission"]').html('');
                    swal({
                        type: 'error',
                        title: '操作失败',
                        text: data.e,
                    })
                }
            },
            error: function () {
                get_status(vm_id, click_element.parents('tr'));
                swal({
                        type: 'error',
                        title: '暂停服务',
                        text: '当前服务不可用',
                    })
                click_element.parents('td').siblings('[name="mission"]').html('');
            },
            headers: {'X-Requested-With': 'XMLHttpRequest'},
        })
    })
    //vnc
    $('.vnc-btn').click(function (event) {
        event.preventDefault();
        if($(this).is('.disabled')){
            return;
        }
        vm_operate = this.value;
        vm_id = $(this).parents('tr').attr('id');
        $.ajax({
            url: "/evcloud/list/",
            type: "POST",
            data: {
                "vm_id": vm_id,
                "vm_operate": vm_operate,
                "csrfmiddlewaretoken": $('[name="csrfmiddlewaretoken"]').val(),
            },
            datatype: "json",
            async: false,
            success: function (data) {
                if (data.code === 200) {
                    window.open(data.e)
                } else {
                    swal({
                        type: 'error',
                        title: '操作失败',
                        text: data.e,
                    })
                }
            },
            error: function () {
                swal({
                        type: 'error',
                        title: '暂停服务',
                        text: '当前服务不可用',
                    })
            },
            headers: {'X-Requested-With': 'XMLHttpRequest'},
        })
    });

    //
    // 虚拟机备注双击修改事件
    //
    $(".vm-remarks").dblclick(function () {
        let remarks = $(this);
        let old_html = remarks.text();

        //如果已经双击过，正在编辑中
        if(remarks.attr('data-in-edit') === 'true'){
            return;
        }
        // 标记正在编辑中
        remarks.attr('data-in-edit', 'true');
        //创建新的input元素，初始内容为原备注信息
        var newobj = document.createElement('input');
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
                // 请求修改备注信息
                data = {};
                data.vm_id = remarks.attr('data-vm-id');
                data.remarks = input_text;
                data.csrfmiddlewaretoken = $('[name="csrfmiddlewaretoken"]').val();
                if(vm_remarks_ajax(data)){

                }else{
                    // 修改失败，显示原内容
                    input_text = old_html;
                    show_warning_dialog('修改备注信息失败', 'error');
                }
            }
            remarks.append(input_text);
        };
    });

    //
    //  同步请求修改备注信息
    //@ data: 请求提交的数据，类型对象
    //@ return:
    //      success: true
    //      failed : false
    function vm_remarks_ajax(data) {
        let ret = false;

        $.ajax({
            url: "/evcloud/remarks/",
            type: "POST",
            data: data,
            content_type: "application/json",
            async: false,
            success: function (res) {
                if(res.code === 200)
                    ret = true;
            },
            headers: {'X-Requested-With': 'XMLHttpRequest'},//django判断是否是异步请求时需要此响应头
        });

        return ret;
    }

})();



