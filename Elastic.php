<?php
namespace App\Model;

use Illuminate\Database\Eloquent\Model; 
use Illuminate\Pagination\LengthAwarePaginator;

class Elastic extends Model
{
	private $options = [
		'host'=>null,
		'port'=>null,
		'url'=>null,
	];
	private $where = null;
	private $index = null;
	private $type = null;
	private static $es = null;

	public function __construct()
	{	
		$options = config('elasticsearch');
		$this->options = array_merge([
			'host' => '127.0.0.1',
			'port' => '9200'
		], $options);
		$this->options['url'] = $this->options['host'] . ':' . $this->options['port'];
	}

	public static function db($index=null)
	{
		if (self::$es == null)
		{
			self::$es = new self();
		}
		if($index != null)
		{
			self::$es->index = $index;
		}
		return self::$es;
	}

	/**
	 * 创建索引
	 * @param  array $option 
	 * $option['index'] ElasticSearch Index Name
	 * $option['type'] ElasticSearch Type Name
	 * $option['properties'] ElasticSearch Properties
	 * @return array
	 */
	public function createIndex($option)
	{

		$json = json_encode([
			'mappings'=>[
				$option['type'] => [
					'properties'=>$option['properties'],
				],
			]
		]);
		$result = $this->curl('PUT', $this->options['url'] . '/' . $option['index'], $json);
		return $this->buildResult($result);
	}

	/**
	 * 删除索引
	 * @param String $index 索引名称
	 * @return array
	 */
	public function deleteIndex(){
		$result = $this->curl('DELETE', $this->options['url'] . '/' . $this->index);
		$this->index = $this->type = null;
		return $this->buildResult($result);
	}

	/**
	 * ElasticSearch服务返回的json数据转换成array格式
	 * @return array
	 * $array['status'] 0：失败，1：成功
	 */
	public function buildResult($result)
	{
		$result = json_decode($result, true);
		if(!isset($result['error'])){
			return ['status'=>1, 'data'=>$result];
		}else{
			return ['status'=>0, 'data'=>$result, 'msg'=>''];
		}
	}

	/**
	 * 添加数据
	 * @param String $id   文档ID
	 * @param Array $data 文档字段内容
	 * @return Array
	 */
	public function add($id,$data){
		$result = $this->curl('POST', $this->options['url'] . '/' . $this->index . '/' . $this->type . '/' . $id,json_encode($data));
		return $this->buildResult($result);
	}

	/**
	 * 查询文档内容
	 * @param String $id   文档ID
	 * @return Array
	 */
	public function get($id){
		$result = $this->curl('GET', $this->options['url'] . '/' . $this->index . '/' . $this->type . '/' . $id);
		return $this->buildResult($result);
	}

	/**
	 * @param Array $option
	 * $option['query'] 查询条件
	 * $option['size'] 查询条数限制
	 */
	public function search($option){
		$size = $option['size']??1;
		$page = request()->input("page",1);
		$from = ($page-1)*$size;
		$this->where['query'] = $option['query'];
		$this->where['from'] = $from;
		$this->where['size'] = $size;
		$result = $this->curl('POST', $this->options['url'] . '/' . $this->index . '/' . $this->type . '/_search', json_encode($this->where));
		$result = $this->buildResult($result);
		$this->where = null;
		$this->index = $this->type = null;
		return new LengthAwarePaginator($result['data']['hits']['hits'], $result['data']['hits']['total'], $size);
	}

	/**
	 * 查看所有索引
	 */
	public function _cat()
	{
		return $this->curl('GET', $this->options['url'] . '/_cat/indices?v');
	}

	public function curl($op,$url,$data=null){
		$ch = curl_init ();
		curl_setopt( $ch, CURLOPT_URL,$url);
		switch($op){
			case 'GET':
				break;
			case 'PUT':
				curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'PUT');
				curl_setopt($ch, CURLOPT_POSTFIELDS, $data);
				break;
			case 'DELETE':
				curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'DELETE');
				break;
			case 'POST':
				curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'POST');
				curl_setopt($ch, CURLOPT_POSTFIELDS, $data);
				break;
		}
		curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
		curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 30);
		curl_setopt($ch, CURLOPT_TIMEOUT, 30);
		curl_setopt($ch, CURLOPT_HTTPHEADER, [
			'Content-Type: application/json',
			'Content-Length:' . strlen($data)
		]);
		if(curl_error($ch)){
			return ['status'=>0, 'msg'=>curl_error($ch)];
		}
		return curl_exec($ch);
	}

	public function index($index)
	{
		$this->index = $index;
		return $this;
	}

	public function type($type)
	{
		$this->type = $type;
		return $this;
	}
}
