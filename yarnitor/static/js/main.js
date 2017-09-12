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
                data: "totalNodes",
                title: "<a title='Active / Total Nodes' href='"+YARN_BASE_URL+"/cluster/nodes'>Nodes</a>",
                render: function(data, type, row) {
                    return (row['activeNodes']) + ' / ' + data;
                }
            },
            {
                data: "totalVirtualCores",
                title: "<a title='Available / Total VCPUs' href='"+YARN_BASE_URL+"/cluster/apps'>VCPUs</a>",
                render: function(data, type, row) {
                    return row['availableVirtualCores'] + ' / ' + data;
                }
            },
            {
               data: "totalMB",
               title: "<a title='Available / Total RAM in GB' href='"+YARN_BASE_URL+"/cluster/scheduler'>RAM (GB)</a>",
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
        colReorder: true,
        // Save column state in the user's browser
        stateSave: true,
        // Keep user column state forever, don't expire it
        stateDuration: 0,
        order: [[4, 'desc']],
        buttons: [
            {
                extend: 'colvis',
                className: 'btn-sm'
            }
        ],
        language: {
            "search": "",
            "searchPlaceholder": "Filter records",
            "buttons": {
                "colvis": '<i class="glyphicon glyphicon-th-list" title="Show/hide columns"></i>'
            }
        },
        dom: ("<'row'<'col-xs-8'l><'col-xs-4 dataTables_controls'Bf>>" +
             "<'row'<'col-xs-12'tr>>" +
             "<'row'<'col-xs-6'i><'col-xs-6 dataTables_refreshed'>>"),
        columns: [
            {
                data: "name",
                title: "Name",
                render: function(data, type, row, meta) {
                    if(type === 'display') {
                        if(data.length > MAX_NAME_LENGTH) {
                            // Abbreviate the name for display, preserving a prefix and suffix
                            data = data.substr(0, MAX_NAME_LENGTH/2) + '...' + data.substr(-MAX_NAME_LENGTH/2);
                        }
                        var html = Mustache.render(name_tmpl, {
                            name: data,
                            trackingUrl: row.trackingUrl,
                            id: row.id
                        });
                        return html;
                    } else {
                        // Return the raw name for sorting, filtering, etc.
                        return data;
                    }
                }
            },
            {data: "user", title: "User"},
            {data: "state", title: "State"},
            {
                title: "Progress",
                sortable: false,
                searchable: false,
                render: function(data, type, row, meta) {
                    if(type === 'display') {
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
                    } else {
                        // Return a string sentinel for type requests
                        return '';
                    }
                }
            },
            {data: "allocatedVCores", title: "VCPUs"},
            {
                data: "vcoreSeconds",
                title: "VCPU-Hours",
                render: function(data, type, row, meta) {
                    return (row.vcoreSeconds / 3600).toFixed(2);
                }
            },
            {
                data: "allocatedMB",
                title: "RAM (GB)",
                render: function(data) {
                    return Math.round(data / 1024);
                }
            },
            {
                data: "memorySeconds",
                title: "RAM (GB)-Hours",
                render: function(data, type, row, meta) {
                    return (row.memorySeconds / 1024 / 3600).toFixed(2);
                }
            },
            {
              title: "RAM/VCPU Ratio",
              render: function(data, type, row, meta) {
                return (row.allocatedMB / row.allocatedVCores / 1024).toFixed(2);
              }
            },
            {"data": "applicationType", "title": "Job Type"},
            {"data": "queue", "title": "Queue"},
            {
                "data": "startedTime",
                "type": "date",
                "title": "Local Start Time",
                "render": function(data, type, row, meta) {
                    return (new Date(data)).toLocaleString();
                }
            },
            {
                "data": "startedTime",
                "title": "Uptime",
                "render": function(data, type, row, meta) {
                    if(type === 'display' || type === 'filter') {
                        return moment.duration(moment.utc() - moment(data)).humanize();
                    } else {
                        return (new Date(data)).getTime();
                    }
                }
            },
            {"data": "id", "title": "Application ID"},
        ]
    });

    var pageScrollPos = 0;

    table.on('search.dt', function() {
        history.replaceState(undefined, undefined, '#'+table.search());
    });

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
        pageScrollPos = $('div.dataTables_scrollBody').scrollTop();
    });

    var reloadDatetime = function() {
        $.get({
            url: YARNITOR_BASE_URL+"/api/status",
            dataType: 'json'
        }).done(function(data) {
            // Show datetime data was last refreshed from the YARN API
            // if that information is available, else leave the display alone
            if(data.refresh_datetime) {
                var d = (new Date(data.refresh_datetime)).toLocaleString();
                $('.dataTables_refreshed').text('Showing data from ' + d);
            }
        });
    }

    setInterval(function() {
        console.log('yarnitor:refresh');
        table.ajax.reload();
        cluster_table.ajax.reload();
        reloadDatetime();
    }, YARNITOR_REFRESH_INTERVAL_S * 1000);

    // Throw exceptions in the console, not in alert dialogs
    $.fn.dataTable.ext.errMode = 'throw';
    // Immediately try to fetch datetime of last data refresh
    reloadDatetime();
    // Set the initial filter text by pulling it from the URL hash
    table.search(window.location.hash.substr(1));

    console.log('yarnitor:dom-ready');
});
