<?php

#TODO: Needs a major cleanup of the code
#TODO: Add class for working with the RADOS objects add/delete/append

class RadosStream {
    var $radosurl = array();
    var $radoscon;

    var $position;
    var $writable;
    var $readable;
    var $truncate;
    var $nonexist_create;
    var $fail_if_exists;

    var $object_prefix = "radosstream_";
    var $header_object;
    var $data_object;
    var $data_object_current_bytes;
    var $content;
    var $metadata;

    var $chunksize = 4194304; //4MB chunksize by default

    var $ioctx;

    function stream_open($path, $mode, $options, &$opened_path)
    {
        $this->parse_rados_url($path);
        $this->radoscon = rados_create();
        rados_conf_read_file($this->radoscon, $this->radosurl['conf']);
        rados_connect($this->radoscon);
        $this->ioctx = rados_ioctx_create($this->radoscon, $this->radosurl['poolname']);

        $status = true;

        switch(substr($mode,0,2)) {

            case "r":
                $this->position = 0;
                $this->writable = false;
                $this->readable = true;
                $this->truncate = false;
                $this->nonexist_create = false;
                $this->fail_if_exists = false;
                break;

            case "r+":
                $this->position = 0;
                $this->writable = true;
                $this->readable = true;
                $this->truncate = false;
                $this->nonexist_create = false;
                $this->fail_if_exists = false;
                break;

            case "w":
                $this->position = 0;
                $this->writable = true;
                $this->readable = false;
                $this->truncate = true;
                $this->nonexist_create = true;
                $this->fail_if_exists = false;
                break;

            case "w+":
                $this->position = 0;
                $this->writable = true;
                $this->readable = true;
                $this->truncate = true;
                $this->nonexist_create = true;
                $this->fail_if_exists = false;
                break;

            case "a":
                $this->position = EOF; //end of file
                $this->writable = true; //writes are always appended
                $this->readable = false; //fseek() does not work
                $this->truncate = false;
                $this->nonexist_create = false;
                $this->fail_if_exists = false;
                break;

            case "a+":
                $this->position = EOF; //end of file
                $this->writable = true; //writes are always appended
                $this->readable = true; //fseek() only works on read position
                $this->truncate = false;
                $this->nonexist_create = true;
                $this->fail_if_exists = false;
                break;

            case "x":
                $this->position = 0;
                $this->writable = true;
                $this->readable = false;
                $this->truncate = false;
                $this->nonexist_create = true;
                $this->fail_if_exists = true; //Generate E_WARNING if exists, return false
                break;

            case "x+":
                $this->position = 0;
                $this->writable = true;
                $this->readable = true;
                $this->truncate = false;
                $this->nonexist_create = true;
                $this->fail_if_exists = true; //Generate E_WARNING if exists, return false
                break;

            case "c":
                $this->position = 0;
                $this->writable = true;
                $this->readable = false;
                $this->truncate = false;
                $this->nonexist_create = true;
                $this->fail_if_exists = false; //WIL NOT Generate E_WARNING if exists
                break;

            case "c+":
                $this->position = 0;
                $this->writable = true;
                $this->readable = true;
                $this->truncate = false;
                $this->nonexist_create = true;
                $this->fail_if_exists = false; //WILL NOT Generate E_WARNING if exists
                break;

            default:
                $status = false;
                break;

        }

        return $status;
    }


    //TODO: Read using RADOS
    function stream_read($count)
    {
        $ret = substr($GLOBALS[$this->varname], $this->position, $count);
        $this->position += strlen($ret);
        return $ret;
    }

    function update_object_details()
    {
        $this->header_object = $this->object_prefix . $this->radosurl['object'] . "_header";
        $this->data_object = ($this->position == 0) ? $this->object_prefix . $this->radosurl['object'] . "_" . sprintf('%08d', 0) : $this->object_prefix . $this->radosurl['object'] . "_" . sprintf('%08d', floor(($this->position / $this->chunksize)));
        $this->data_object_current_bytes = ($this->position == 0) ? 0 : $this->position - (floor($this->position / $this->chunksize) * $this->chunksize);
        echo "Header Object: $this->header_object \n";
        echo "Data Object: $this->data_object \n";
        echo "Data Object Current Bytes: $this->data_object_current_bytes \n";
    }

    function stream_write($data)
    {
        $written = 0;
        $length = strlen($data) - 1;
        while($written <= $length) {
            $this->update_object_details();
            $size = 0;
            $start = $size;
            $data = substr($data, $written, $this->chunksize);
            $hash = sha1($data);
            $write = rados_write($this->ioctx, $this->data_object, $data, $this->data_object_current_bytes);
            $size += strlen($data);
            $written += $size;
            $this->metadata[$this->data_object] = array('start' => $this->position, 'size' => $size, 'hash' => $hash);
            $this->position += $written;
            echo "Size of Data: " . $size . "\n";
            echo "Bytes Written: " . $written . "\n";
            echo "Position: " . $this->position . "\n";
        }
        $header_data = json_encode(array('parts' => count($this->metadata), 'metadata' => $this->metadata, 'size' => $this->position), JSON_PRETTY_PRINT) . "\n";
        rados_write_full($this->ioctx, $this->header_object, $header_data);
        return $written;
    }

    function stream_tell()
    {
        return $this->position;
    }

    function stream_eof()
    {
        return $this->position >= strlen($GLOBALS[$this->varname]);
    }

    function stream_seek($offset, $whence)
    {
        switch ($whence) {
            case SEEK_SET:
                if ($offset < strlen($GLOBALS[$this->varname]) && $offset >= 0) {
                    $this->position = $offset;
                    return true;
                } else {
                    return false;
                }
                break;

            case SEEK_CUR:
                if ($offset >= 0) {
                    $this->position += $offset;
                    return true;
                } else {
                    return false;
                }
                break;

            case SEEK_END:
                if (strlen($GLOBALS[$this->varname]) + $offset >= 0) {
                    $this->position = strlen($GLOBALS[$this->varname]) + $offset;
                    return true;
                } else {
                    return false;
                }
                break;

            default:
                return false;
        }
    }

    function stream_metadata($path, $option, $var)
    {
        if($option == STREAM_META_TOUCH) {
            $url = parse_url($path);
            $varname = $url["host"];
            if(!isset($GLOBALS[$varname])) {
                $GLOBALS[$varname] = '';
            }
            return true;
        }
        return false;
    }

    #TODO: what should this look like?
    # SYNTAX:  rados://client:/path/to/ceph.conf@poolname/objectname
    # EXAMPLE: rados://admin:/etc/ceph/ceph.conf@phprados/test
    function parse_rados_url($path)
    {

        $status = false; //assuming failure for sanity reasons

        if(substr($path, 0, 8) == "rados://") {
            $details = substr($path, 8);
            $split_1 = explode("@", $details);
            $client_and_conf = $split_1[0];
            list($client, $conf) = explode(":", $client_and_conf);
            $pool_and_object = $split_1[1];
            list($pool, $object) = explode("/", $pool_and_object);
        }

        $path_parts = array("client" => $client, "conf" => $conf, "poolname" => $pool, "object" => $object);

        foreach($path_parts as $key => $value) {
            if(strlen($value) <= 0) {
                $status = false; //we should be raising an exception at this point
            }
        }

        $this->radosurl = $path_parts;
        $status = true;
        return $status;

    }

}

stream_wrapper_register("rados", "RadosStream") or die("Failed to register protocol");

?>