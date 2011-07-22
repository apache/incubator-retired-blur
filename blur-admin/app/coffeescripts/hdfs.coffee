$(document).ready ->
  # method to initialize the jstree
  setup_file_tree = () ->
    $('.file_layout').jstree
      plugins: ["themes", "html_data", "sort", "ui"],
      themes:
        theme: 'apple',
    $('.file_layout').bind "loaded.jstree", ->
      $('#hdfs_files').show()

  setup_file_tree()
  view = 'view1'

  $('#hdfs_files a').live 'click', ->
    new_data(this.id)

  change_view = () ->
    if view == 'view1'
      $('#file_tiles').hide()
      $('#file_list').show()
    else
      $('#file_list').hide()
      $('#file_tiles').show()

  new_data = (id) ->
    $.ajax '/hdfs/'+ id ,
      type: 'POST',
      success: (data) ->
        $('#data_container_display').html data
        change_view()
        $.each($("#file_tiles > button" ), ->
          $('#file_tiles #' + this.id).button()
        )

  $('#view_options').live 'change', ->
    view = $('#view_options').find(':checked').attr('value')
    change_view()

  $('#file_tiles > .ui-button').live 'click', ->
    new_data(this.id)

  $('#view_options').buttonset()
  $.each($("#toolbar > button" ), ->
    $('#toolbar #' + this.id).button()
  )





  
