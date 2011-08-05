$(document).ready ->
  # method to initialize the jstree
  setup_file_tree = () ->
    $('.file_layout').jstree
      plugins: ["themes", "html_data", "sort", "ui", "search" ],
      themes:
        theme: 'apple',
    $('.file_layout').bind "loaded.jstree", ->
      $('#hdfs_files').show()
    $('#search_button').live "click", ->
      $('.file_layout').jstree "search", $('#search_string').val()
    $('.file_layout').bind "search.jstree", (e, data) ->
      alert "Found " + data.rslt.nodes.length + " nodes matching '" + data.rslt.str + "'."

  setup_file_tree()

  $('#hdfs_files a').live 'click', ->
    new_data(this.id)

  change_view = () ->
    view = $('input:radio:checked').val()
    if view == 'list'
      $('#file_tiles').hide()
      $('#file_list').show()
    else if view == 'icon'
      $('#file_list').hide()
      $('#file_tiles').show()

  new_data = (id) ->
    file = $('#'+ id).attr('name').replace(/\//g," ").replace('.','*')
    connection = $('#'+ id).attr('connection')
    $.ajax '/hdfs/' + file + '/' + connection,
      type: 'POST',
      success: (data) ->
        $('#data_container_display').html data
        change_view()
        $.each($("#file_tiles > button" ), ->
          $('#file_tiles #' + this.id).button()
        )
    $('#location_string').text $('#'+ id).attr('name')

  $('#view_options').live 'change', ->
    view = $('#view_options').find(':checked').attr('value')
    change_view()

  $('#file_tiles > .ui-button').live 'click', ->
    new_data(this.id)
  $('#file_list a').live 'click', ->
    new_data(this.id)

  $('#view_options').buttonset()
  $.each($("#toolbar > button" ), ->
    $('#toolbar #' + this.id).button()
  )

  # make jstree with json
  setup_file2_tree = () ->
    #alert 'file2'
    $('#hdfs_files_json').jstree
      json_data:
          ajax:
            url: 'hdfs/make/jstree/',
            type: 'POST',
            dataType: "json",
          
      plugins: ["themes", "json_data", "sort", "ui"],
      themes:
        theme: 'apple',


  #setup_file2_tree()
