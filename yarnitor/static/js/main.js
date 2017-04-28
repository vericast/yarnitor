$(document).ready(function() {
    'use strict';

    var MAX_NAME_LENGTH = 50;
    var BANNER_HEIGHT = 160;

    // Prep templates located in the HTML
    var name_tmpl = $('#name_tmpl').html();
    var progress_tmpl = $('#progress_tmpl').html();
    var progress_container_tmpl = $('#progress_container_tmpl').html();
    var progress_popover_tmpl = $('#progress_popover_tmpl').html();

    // Compile templates for speedup
    Mustache.parse(name_tmpl);
    Mustache.parse(progress_tmpl);
    Mustache.parse(progress_container_tmpl);
    Mustache.parse(progress_popover_tmpl);

    // Handle vertical resizing of the table
    function calcHeight() {
        return $(window).height() - BANNER_HEIGHT;
    }

    $(window).on('resize', function() {
        // The joy of fitting vertically!
        $('.dataTables_scrollBody').height(calcHeight()).css('max-height', 'inherit');
    });

    // Cluster status table.
    var cluster_table = $("#yarn_status").DataTable({
        ajax: YARNITOR_BASE_URL+"/api/cluster",
        paging: false,
        searching: false,
        ordering: false,
        dom: 't',
        columns: [
            {
                data: "activeNodes",
                title: "<a href='"+YARN_BASE_URL+"/cluster/nodes'>Nodes</a>",
                render: function(data, type, row) {
                    return (data - row['unhealthyNodes']) + ' / ' + data;
                }
            },
            {
                data: "totalVirtualCores",
                title: "<a href='"+YARN_BASE_URL+"/cluster/apps'>VCPUs</a>",
                render: function(data, type, row) {
                    return row['availableVirtualCores'] + ' / ' + data;
                }
            },
            {
               data: "totalMB",
               title: "<a href='"+YARN_BASE_URL+"/cluster/scheduler'>RAM (GB)</a>",
               render: function(data, type, row) {
                    return Math.round(row['availableMB'] / 1024) + ' / ' + Math.round(data / 1024);
               }
            }
        ]
    });

    // Main applications table
    var table = $('#yarn_applications').DataTable({
        ajax: YARNITOR_BASE_URL+"/api/applications",
        rowId: "id",
        scrollX: true,
        scrollY: calcHeight(),
        paging: false,
        order: [4, 'desc'],
        language: {
            "search": "",
            "searchPlaceholder": "Filter records"
        },
        dom: ("<'row'<'col-xs-8'l><'col-xs-4'f>>" +
             "<'row'<'col-xs-12'tr>>" +
             "<'row'<'col-xs-6'i><'col-xs-6 dataTables_refreshed'>>"),
        columns: [
            {
                data: "name",
                title: "Name",
                render: function(data, type, row, meta) {
                    if(data.length > MAX_NAME_LENGTH) {
                        data = data.substr(0, MAX_NAME_LENGTH) + '...';
                    }
                    var html = Mustache.render(name_tmpl, {
                        name: data,
                        trackingUrl: row.trackingUrl,
                        id: row.id
                    });
                    return html;
                }
            },
            {data: "user", title: "User"},
            {data: "state", title: "State"},
            {
                title: "Progress",
                render: function(data, type, row, meta) {
                    // progress bars
                    var bar_htmls = row.progress.map(function(progress) {
                        var total = progress.completed + progress.running + progress.failed;
                        total = (progress.total > total) ? progress.total : total;
                        var args = {
                            completed: progress.completed,
                            completed_ratio: progress.completed / progress.total * 100,
                            running: progress.running,
                            running_ratio: progress.running / progress.total * 100,
                            failed: progress.failed,
                            failed_ratio: progress.failed / progress.total * 100,
                            total: progress.total
                        };

                        return Mustache.render(progress_tmpl, args);
                    });

                    return Mustache.render(progress_container_tmpl, {
                        progress_html: bar_htmls.join('\n'),
                        popover_html : Mustache.render(progress_popover_tmpl, row)
                    });
                }
            },
            {data: "allocatedVCores", title: "VCPUs"},
            // TODO: human readable
            {
                data: "allocatedMB",
                title: "Memory (GB)",
                render: function(data) {
                    return Math.round(data / 1024);
                }
            },
            {
              "title": "Mem/VCPU Ratio",
              "render": function(data, type, row, meta) {
                return (row.allocatedMB / row.allocatedVCores / 1024).toFixed(2);
              }
            },
            {"data": "applicationType", "title": "Job Type"},
            {"data": "queue", "title": "Queue"},
            {
                "data": "startedTime",
                "title": "Local Start Time",
                "render": function(data, type, row, meta) {
                    return (new Date(data)).toLocaleString();
                }
            },
        ]
    });

    var pageScrollPos = 0;

    table.on('draw.dt', function() {
        console.log('yarnitor:draw.dt');
        // Remove shown popovers
        $('.popover').remove();
        $('[data-toggle="popover"]').popover();
        // rescroll
        $('div.dataTables_scrollBody').scrollTop(pageScrollPos);
    });

    table.on('xhr.dt', function() {
        console.log('yarnitor:xhr.dt');
        // destroy the popovers before the draw phase kicks in.
        $('[data-toggle="popover"]').popover("destroy");
        var now = (new Date()).toLocaleString();
        $('.dataTables_refreshed').text('Refreshed ' + now);
        pageScrollPos = $('div.dataTables_scrollBody').scrollTop();
    });

    setInterval(function() {
        table.ajax.reload();
        cluster_table.ajax.reload();
        console.log('yarnitor:refresh');
    }, YARNITOR_REFRESH_INTERVAL_S * 1000);

    // Throw exceptions in the console, not in alert dialogs
    $.fn.dataTable.ext.errMode = 'throw';

    console.log('yarnitor:dom-ready');
});
