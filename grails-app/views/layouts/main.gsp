<!DOCTYPE html>
<html>
    <head>
        <title><g:layoutTitle default="Miax Monitor" /></title>
        <link rel="stylesheet" href="${resource(dir:'css',file:'main.css')}" />
        <link rel="shortcut icon" href="${resource(dir:'images',file:'favicon.ico')}" type="image/x-icon" />
        <g:javascript library="jquery-1.7.1.min" />
        <g:javascript library="jquery-ui-1.8.16.custom/js/jquery-ui-1.8.16.custom.min" />
        <g:javascript library="jquery.dataTables.min"/>
        <g:layoutHead />
    </head>
    <body>
        <div class="miax_page">
            <div class="miax_page_banner">
                <div class="miax_page_banner_color"></div>
                <a class="miax_logo" href="http://www.miaxoptions.com"></a>
            </div>
            <div class="miax_page_content">
              <g:layoutBody />
            </div>
        </div>
    </body>
</html>