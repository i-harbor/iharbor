;(function() {

    $('#btn-evcloud-add').click(function () {
        self.location.href = "/evcloud/add";
    })

    function get_status(url, vm_id, click_element) {
        $.ajax({
            url: url,
            type: "POST",
            data: {
                "vm_id": vm_id,
                "vm_operate": vm_operate,
                "csrfmiddlewaretoken": $('[name="csrfmiddlewaretoken"]').val(),
            },
            datatype: "json",
            success: function (data) {
                if(data.code === 200){
                    //click_element.parents('td').siblings('[name="status"]').html(data.status);
                    //click_element.parents('td').siblings('[name="mission"]').html('');
                }
            },
            headers: {'X-Requested-With': 'XMLHttpRequest'},
        })
    }
    //
    // 全选/全不选
    //
    $(':checkbox[data-check-target]').click(function () {
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
    $('.item-checkbox').click(function () {
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

    $('.vm-operate').click(function (event) {
        event.preventDefault();
        let operate_list = ['启动', '关闭', '断电', '重启' ,'删除'];
        click_element = $(this);
        vm_operate = event.target.getAttribute('value');
        vm_id = click_element.parents('tr').attr('id');
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
                if(data.code === 200){
                    click_element.parents('td').siblings('[name="status"]').html(data.status);
                    click_element.parents('td').siblings('[name="mission"]').html('');
                }


            },
            headers: {'X-Requested-With': 'XMLHttpRequest'},
        })
    })

    $('.vnc-btn').click(function (event) {
        event.preventDefault();
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
                }


            },
            headers: {'X-Requested-With': 'XMLHttpRequest'},
        })
    })


})();



